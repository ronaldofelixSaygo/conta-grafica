const express = require('express');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const initSqlJs = require('sql.js');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const XLSX = require('xlsx');
// pdfjs-dist loaded lazily in route to avoid canvas polyfill warnings on startup
async function extractPdfText(buffer) {
  const pdfjs = require('pdfjs-dist/legacy/build/pdf.js');
  const data = new Uint8Array(buffer);
  const doc = await pdfjs.getDocument({ data, useSystemFonts: true, disableFontFace: true }).promise;
  let text = '';
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const tc = await page.getTextContent();
    let lastY = null;
    let lineParts = [];
    for (const it of tc.items) {
      const y = it.transform[5];
      if (lastY !== null && Math.abs(y - lastY) > 2) {
        text += lineParts.join(' ') + '\n';
        lineParts = [];
      }
      lineParts.push(it.str);
      lastY = y;
    }
    if (lineParts.length) text += lineParts.join(' ') + '\n';
    text += '\n';
  }
  return text;
}

const app = express();
const PORT = process.env.PORT || 3000;
const DB_PATH = path.join(__dirname, 'database.sqlite');

// Database abstraction layer
let db = null;
let isPostgres = false;
let pgPool = null;

// Helper function to execute queries with abstraction
const dbQuery = async (sql, params = []) => {
  if (isPostgres) {
    try {
      // Convert sql.js ? placeholders to pg $1, $2 format
      let pgSql = sql;
      let paramIndex = 1;
      while (pgSql.includes('?')) {
        pgSql = pgSql.replace('?', `$${paramIndex}`);
        paramIndex++;
      }
      const result = await pgPool.query(pgSql, params);
      return result;
    } catch (error) {
      console.error('PostgreSQL query error:', error);
      throw error;
    }
  } else {
    // sql.js - synchronous execution
    return db.exec(sql, params);
  }
};

// Helper function to execute write operations
const dbRun = async (sql, params = []) => {
  if (isPostgres) {
    let pgSql = sql;
    let paramIndex = 1;
    while (pgSql.includes('?')) {
      pgSql = pgSql.replace('?', `$${paramIndex}`);
      paramIndex++;
    }
    try {
      await pgPool.query(pgSql, params);
      return { changes: 1 };
    } catch (error) {
      console.error('PostgreSQL run error:', error);
      throw error;
    }
  } else {
    db.run(sql, params);
    return { changes: 1 };
  }
};

// Convert sql.js results to standardized format
const formatResult = (result, isQuery = true) => {
  if (isPostgres) {
    return result.rows;
  } else {
    if (result.length === 0) return [];
    const cols = result[0].columns;
    return result[0].values.map(row => {
      const obj = {};
      cols.forEach((c, i) => obj[c] = row[i]);
      return obj;
    });
  }
};

function saveDb() {
  if (!isPostgres && db) {
    const data = db.export();
    const buffer = Buffer.from(data);
    fs.writeFileSync(DB_PATH, buffer);
  }
  // PostgreSQL persists automatically, no-op
}

async function initDatabase() {
  if (isPostgres) {
    // PostgreSQL schema creation
    const createTablesSQL = [
      `CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        role TEXT DEFAULT 'user',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE IF NOT EXISTS clientes (
        id SERIAL PRIMARY KEY,
        nome TEXT NOT NULL,
        cnpj TEXT,
        cnpj_filial TEXT,
        escritorio TEXT,
        locacao_sala TEXT DEFAULT '',
        abertura_filial TEXT DEFAULT '',
        reativacao_ie TEXT DEFAULT '',
        conta_grafica TEXT DEFAULT '',
        cliente_certificado TEXT DEFAULT '',
        parceiro_sala TEXT,
        parceiro_filial TEXT,
        parceiro_ie TEXT,
        observacoes TEXT,
        percentual_comissao REAL DEFAULT 0,
        dia_fechamento INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE IF NOT EXISTS movimentacoes (
        id SERIAL PRIMARY KEY,
        cliente_id INTEGER NOT NULL,
        tipo_movimento TEXT NOT NULL,
        data_nf TEXT,
        duimp_di_processo TEXT,
        parceiro TEXT,
        data_exoneracao TEXT,
        percentual REAL,
        valor REAL DEFAULT 0,
        valor_ajustado REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (cliente_id) REFERENCES clientes(id)
      )`,
      `CREATE TABLE IF NOT EXISTS audit_log (
        id SERIAL PRIMARY KEY,
        user_id INTEGER,
        user_name TEXT,
        action TEXT,
        entity TEXT,
        entity_id INTEGER,
        details TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`
    ];

    for (const sql of createTablesSQL) {
      try {
        await pgPool.query(sql);
      } catch (e) {
        // Table likely exists, continue
        console.log('Table creation note:', e.message.split('\n')[0]);
      }
    }

    // Add missing columns if they don't exist (PostgreSQL)
    const checkColumnSQL = `
      SELECT column_name FROM information_schema.columns
      WHERE table_name = 'clientes' AND column_name = $1
    `;

    const columnsToAdd = [
      { name: 'cnpj', type: 'TEXT' },
      { name: 'cnpj_filial', type: 'TEXT' },
      { name: 'parceiro_sala', type: 'TEXT' },
      { name: 'parceiro_filial', type: 'TEXT' },
      { name: 'parceiro_ie', type: 'TEXT' },
      { name: 'percentual_comissao', type: 'REAL DEFAULT 0' },
      { name: 'dia_fechamento', type: 'INTEGER DEFAULT 1' }
    ];

    for (const col of columnsToAdd) {
      try {
        const checkResult = await pgPool.query(checkColumnSQL, [col.name]);
        if (checkResult.rows.length === 0) {
          await pgPool.query(`ALTER TABLE clientes ADD COLUMN ${col.name} ${col.type}`);
          console.log(`Column ${col.name} added to clientes table`);
        }
      } catch (e) {
        console.log(`Column ${col.name} check: ${e.message}`);
      }
    }
  } else {
    // sql.js schema creation
    db.run(`CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL,
      role TEXT DEFAULT 'user',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS clientes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nome TEXT NOT NULL,
      cnpj TEXT,
      cnpj_filial TEXT,
      escritorio TEXT,
      locacao_sala TEXT DEFAULT '',
      abertura_filial TEXT DEFAULT '',
      reativacao_ie TEXT DEFAULT '',
      conta_grafica TEXT DEFAULT '',
      cliente_certificado TEXT DEFAULT '',
      parceiro_sala TEXT,
      parceiro_filial TEXT,
      parceiro_ie TEXT,
      observacoes TEXT,
      percentual_comissao REAL DEFAULT 0,
      dia_fechamento INTEGER DEFAULT 1,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS movimentacoes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      cliente_id INTEGER NOT NULL,
      tipo_movimento TEXT NOT NULL,
      data_nf TEXT,
      duimp_di_processo TEXT,
      parceiro TEXT,
      data_exoneracao TEXT,
      percentual REAL,
      valor REAL DEFAULT 0,
      valor_ajustado REAL DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (cliente_id) REFERENCES clientes(id)
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER,
      user_name TEXT,
      action TEXT,
      entity TEXT,
      entity_id INTEGER,
      details TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Add new columns to clientes table if they don't exist
    try {
      db.run(`ALTER TABLE clientes ADD COLUMN parceiro_sala TEXT`);
    } catch (e) {
      // Column already exists
    }
    try {
      db.run(`ALTER TABLE clientes ADD COLUMN parceiro_filial TEXT`);
    } catch (e) {
      // Column already exists
    }
    try {
      db.run(`ALTER TABLE clientes ADD COLUMN parceiro_ie TEXT`);
    } catch (e) {
      // Column already exists
    }
    try {
      db.run(`ALTER TABLE clientes ADD COLUMN percentual_comissao REAL DEFAULT 0`);
    } catch (e) {
      // Column already exists
    }
    try {
      db.run(`ALTER TABLE clientes ADD COLUMN dia_fechamento INTEGER DEFAULT 1`);
    } catch (e) {
      // Column already exists
    }
    try {
      db.run(`ALTER TABLE clientes ADD COLUMN cnpj TEXT`);
    } catch (e) {
      // Column already exists
    }
    try {
      db.run(`ALTER TABLE clientes ADD COLUMN cnpj_filial TEXT`);
    } catch (e) {
      // Column already exists
    }
  }

  // Default users that are always ensured on startup
  const defaultUsers = [
    { name: 'Administrador', email: 'admin@saygogroup.com.br', password: 'admin123', role: 'admin' },
    { name: 'Ronaldo Felix', email: 'ronaldo.felix@saygogroup.com.br', password: '123456', role: 'admin' }
  ];

  // Additional users from environment variable INITIAL_USERS (JSON array)
  if (process.env.INITIAL_USERS) {
    try {
      const extra = JSON.parse(process.env.INITIAL_USERS);
      if (Array.isArray(extra)) defaultUsers.push(...extra);
    } catch (e) {
      console.error('Erro ao parsear INITIAL_USERS:', e.message);
    }
  }

  // Ensure all default/initial users exist
  for (const u of defaultUsers) {
    try {
      const result = await dbQuery("SELECT id FROM users WHERE email = ?", [u.email]);
      const exists = isPostgres ? result.rows.length > 0 : result.length > 0 && result[0].values.length > 0;

      if (!exists) {
        const hash = bcrypt.hashSync(u.password, 10);
        await dbRun("INSERT INTO users (name, email, password, role) VALUES (?,?,?,?)",
          [u.name, u.email, hash, u.role || 'user']);
        console.log(`Usuário criado automaticamente: ${u.name} (${u.email})`);
      }
    } catch (e) {
      console.error(`Error ensuring user ${u.email}:`, e.message);
    }
  }

  saveDb();

  // AUTO-MIGRATE: If using PostgreSQL and tables are empty, import data from SQLite file
  if (isPostgres) {
    try {
      const clientCount = await pgPool.query("SELECT COUNT(*) FROM clientes");
      if (parseInt(clientCount.rows[0].count) === 0) {
        console.log('PostgreSQL tables are empty. Attempting auto-migration from SQLite...');
        const sqlitePath = path.join(__dirname, 'database.sqlite');
        if (fs.existsSync(sqlitePath)) {
          const SQL = await initSqlJs();
          const fileBuffer = fs.readFileSync(sqlitePath);
          const sqliteDb = new SQL.Database(fileBuffer);

          // Migrate clientes
          const clientes = sqliteDb.exec("SELECT * FROM clientes ORDER BY id");
          if (clientes.length > 0) {
            const cols = clientes[0].columns;
            for (const row of clientes[0].values) {
              const obj = {};
              cols.forEach((c, i) => obj[c] = row[i]);
              try {
                await pgPool.query(
                  `INSERT INTO clientes (id, nome, cnpj_filial, escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes, percentual_comissao, dia_fechamento)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                   ON CONFLICT (id) DO NOTHING`,
                  [obj.id, obj.nome, obj.cnpj_filial || '', obj.escritorio, obj.locacao_sala, obj.abertura_filial, obj.reativacao_ie, obj.conta_grafica, obj.cliente_certificado, obj.parceiro_sala || '', obj.parceiro_filial || '', obj.parceiro_ie || '', obj.observacoes || '', obj.percentual_comissao || 0, obj.dia_fechamento || 1]
                );
              } catch (e) { console.error('Migrate cliente error:', e.message); }
            }
            // Reset sequence to max id
            await pgPool.query("SELECT setval('clientes_id_seq', (SELECT COALESCE(MAX(id),1) FROM clientes))");
            console.log(`Migrated ${clientes[0].values.length} clientes`);
          }

          // Migrate movimentacoes
          const movs = sqliteDb.exec("SELECT * FROM movimentacoes ORDER BY id");
          if (movs.length > 0) {
            const cols = movs[0].columns;
            let count = 0;
            for (const row of movs[0].values) {
              const obj = {};
              cols.forEach((c, i) => obj[c] = row[i]);
              try {
                await pgPool.query(
                  `INSERT INTO movimentacoes (id, cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor, valor_ajustado)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                   ON CONFLICT (id) DO NOTHING`,
                  [obj.id, obj.cliente_id, obj.tipo_movimento, obj.data_nf, obj.duimp_di_processo || '', obj.parceiro || '', obj.data_exoneracao || null, obj.percentual || null, obj.valor || 0, obj.valor_ajustado || 0]
                );
                count++;
              } catch (e) { /* skip duplicate */ }
            }
            // Reset sequence
            await pgPool.query("SELECT setval('movimentacoes_id_seq', (SELECT COALESCE(MAX(id),1) FROM movimentacoes))");
            console.log(`Migrated ${count} movimentacoes`);
          }

          sqliteDb.close();
          console.log('Auto-migration from SQLite completed!');
        } else {
          console.log('No SQLite file found for migration. Starting with empty database.');
        }
      } else {
        console.log(`PostgreSQL already has ${clientCount.rows[0].count} clientes. Skipping migration.`);
      }
    } catch (e) {
      console.error('Auto-migration error:', e.message);
    }
  }
}

app.set('trust proxy', 1);
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));
app.use(session({
  secret: process.env.SESSION_SECRET || 'conta-grafica-secret-key-2026',
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 24 * 60 * 60 * 1000,
    httpOnly: true,
    sameSite: 'lax'
  }
}));

// Ensure /api/* errors always return JSON (not HTML) so the frontend shows proper messages
app.use('/api', (req, res, next) => {
  res.setHeader('Cache-Control', 'no-store');
  next();
});

const upload = multer({ dest: 'uploads/' });

// Auth middleware
function requireAuth(req, res, next) {
  if (!req.session.user) return res.status(401).json({ error: 'Não autorizado' });
  next();
}

function requireAdmin(req, res, next) {
  if (!req.session.user || req.session.user.role !== 'admin')
    return res.status(403).json({ error: 'Acesso negado' });
  next();
}

async function logAction(userId, userName, action, entity, entityId, details) {
  await dbRun("INSERT INTO audit_log (user_id, user_name, action, entity, entity_id, details) VALUES (?,?,?,?,?,?)",
    [userId, userName, action, entity, entityId, details]);
  saveDb();
}

// ============ AUTH ROUTES ============
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const result = await dbQuery("SELECT * FROM users WHERE email = ?", [email]);
    const users = formatResult(result);
    if (users.length === 0)
      return res.status(401).json({ error: 'Email ou senha inválidos' });

    const user = users[0];
    if (!bcrypt.compareSync(password, user.password))
      return res.status(401).json({ error: 'Email ou senha inválidos' });

    req.session.user = { id: user.id, name: user.name, email: user.email, role: user.role };
    res.json({ user: req.session.user });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/auth/logout', (req, res) => {
  req.session.destroy();
  res.json({ ok: true });
});

app.get('/api/auth/me', (req, res) => {
  if (!req.session.user) return res.status(401).json({ error: 'Não autenticado' });
  res.json({ user: req.session.user });
});

// ============ USER ROUTES ============
app.get('/api/users', requireAuth, requireAdmin, async (req, res) => {
  try {
    const result = await dbQuery("SELECT id, name, email, role, created_at FROM users ORDER BY name");
    const users = formatResult(result);
    res.json(users);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/users', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { name, email, password, role } = req.body;
    const hash = bcrypt.hashSync(password, 10);
    await dbRun("INSERT INTO users (name, email, password, role) VALUES (?,?,?,?)", [name, email, hash, role || 'user']);
    saveDb();
    await logAction(req.session.user.id, req.session.user.name, 'CREATE', 'user', null, `Usuário criado: ${name}`);
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: 'Email já cadastrado' });
  }
});

app.delete('/api/users/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    await dbRun("DELETE FROM users WHERE id = ? AND id != ?", [req.params.id, req.session.user.id]);
    saveDb();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ CLIENTES ROUTES ============
app.get('/api/clientes', requireAuth, async (req, res) => {
  try {
    const result = await dbQuery("SELECT * FROM clientes ORDER BY nome");
    const items = formatResult(result);
    res.json(items);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/clientes', requireAuth, async (req, res) => {
  try {
    const { nome, cnpj, cnpj_filial, escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes, percentual_comissao, dia_fechamento } = req.body;
    await dbRun(`INSERT INTO clientes (nome, cnpj, cnpj_filial, escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes, percentual_comissao, dia_fechamento)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, [nome, cnpj || '', cnpj_filial || '', escritorio, locacao_sala || '', abertura_filial || '', reativacao_ie || '', conta_grafica || '', cliente_certificado || '', parceiro_sala || '', parceiro_filial || '', parceiro_ie || '', observacoes || '', parseFloat(percentual_comissao) || 0, parseInt(dia_fechamento) || 1]);
    saveDb();
    await logAction(req.session.user.id, req.session.user.name, 'CREATE', 'cliente', null, `Cliente criado: ${nome}`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ COMISSAO EM LOTE (must be before :id routes) ============
app.put('/api/clientes/comissao-lote', requireAuth, async (req, res) => {
  try {
    const { ids, percentual_comissao, dia_fechamento } = req.body;
    if (!ids || !Array.isArray(ids) || ids.length === 0) return res.status(400).json({ error: 'Nenhum cliente selecionado' });

    const updates = [];
    const params = [];
    if (percentual_comissao !== null && percentual_comissao !== '') {
      updates.push('percentual_comissao = ?');
      params.push(parseFloat(percentual_comissao));
    }
    if (dia_fechamento !== null && dia_fechamento !== '') {
      updates.push('dia_fechamento = ?');
      params.push(parseInt(dia_fechamento));
    }
    if (updates.length === 0) return res.status(400).json({ error: 'Nenhum campo para atualizar' });

    updates.push('updated_at = CURRENT_TIMESTAMP');
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE clientes SET ${updates.join(', ')} WHERE id IN (${placeholders})`;
    await dbRun(sql, [...params, ...ids]);
    saveDb();
    await logAction(req.session.user.id, req.session.user.name, 'UPDATE', 'cliente', null, `Comissão atualizada em lote: ${ids.length} clientes`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put('/api/clientes/:id', requireAuth, async (req, res) => {
  try {
    const { nome, cnpj, cnpj_filial, escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes, percentual_comissao, dia_fechamento } = req.body;
    await dbRun(`UPDATE clientes SET nome=?, cnpj=?, cnpj_filial=?, escritorio=?, locacao_sala=?, abertura_filial=?, reativacao_ie=?, conta_grafica=?, cliente_certificado=?, parceiro_sala=?, parceiro_filial=?, parceiro_ie=?, observacoes=?, percentual_comissao=?, dia_fechamento=?, updated_at=CURRENT_TIMESTAMP
      WHERE id=?`, [nome, cnpj || '', cnpj_filial || '', escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes, parseFloat(percentual_comissao) || 0, parseInt(dia_fechamento) || 1, req.params.id]);
    saveDb();
    await logAction(req.session.user.id, req.session.user.name, 'UPDATE', 'cliente', req.params.id, `Cliente atualizado: ${nome}`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/clientes/:id', requireAuth, async (req, res) => {
  try {
    const result = await dbQuery("SELECT nome FROM clientes WHERE id = ?", [req.params.id]);
    const rows = formatResult(result);
    const nome = rows.length > 0 ? rows[0].nome : 'N/A';

    await dbRun("DELETE FROM movimentacoes WHERE cliente_id = ?", [req.params.id]);
    await dbRun("DELETE FROM clientes WHERE id = ?", [req.params.id]);
    saveDb();
    await logAction(req.session.user.id, req.session.user.name, 'DELETE', 'cliente', req.params.id, `Cliente excluído: ${nome}`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ MOVIMENTACOES ROUTES ============
app.get('/api/movimentacoes', requireAuth, async (req, res) => {
  try {
    const { cliente_id, page = 1, limit = 50, search,
            f_cliente, f_tipo, f_duimp, f_parceiro,
            f_data_ini, f_data_fim, f_valor_min, f_valor_max,
            sort_by, sort_dir } = req.query;
    const likeOp = isPostgres ? 'ILIKE' : 'LIKE';
    let sql = `SELECT m.*, c.nome as cliente_nome FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id`;
    const params = [];
    const conditions = [];

    if (cliente_id) { conditions.push("m.cliente_id = ?"); params.push(cliente_id); }
    if (search) { conditions.push(`(c.nome ${likeOp} ? OR m.duimp_di_processo ${likeOp} ?)`); params.push(`%${search}%`, `%${search}%`); }
    if (f_cliente) { conditions.push(`c.nome ${likeOp} ?`); params.push(`%${f_cliente}%`); }
    if (f_tipo) { conditions.push(`m.tipo_movimento ${likeOp} ?`); params.push(`%${f_tipo}%`); }
    if (f_duimp) { conditions.push(`m.duimp_di_processo ${likeOp} ?`); params.push(`%${f_duimp}%`); }
    if (f_parceiro) { conditions.push(`m.parceiro ${likeOp} ?`); params.push(`%${f_parceiro}%`); }
    if (f_data_ini) { conditions.push("m.data_nf >= ?"); params.push(f_data_ini); }
    if (f_data_fim) { conditions.push("m.data_nf <= ?"); params.push(f_data_fim); }
    if (f_valor_min) { conditions.push("m.valor_ajustado >= ?"); params.push(parseFloat(f_valor_min)); }
    if (f_valor_max) { conditions.push("m.valor_ajustado <= ?"); params.push(parseFloat(f_valor_max)); }

    if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");

    // Count total
    const countSql = sql.replace("m.*, c.nome as cliente_nome", "COUNT(*) as total");
    const countResult = await dbQuery(countSql, params);
    const total = isPostgres ? (countResult.rows[0]?.total || 0) : (countResult[0]?.values[0][0] || 0);

    // Sort whitelist
    const sortMap = {
      cliente_nome: 'c.nome',
      tipo_movimento: 'm.tipo_movimento',
      data_nf: 'm.data_nf',
      duimp_di_processo: 'm.duimp_di_processo',
      parceiro: 'm.parceiro',
      valor_ajustado: 'm.valor_ajustado'
    };
    const sortCol = sortMap[sort_by] || 'm.data_nf';
    const sortD = (sort_dir && sort_dir.toUpperCase() === 'ASC') ? 'ASC' : 'DESC';
    sql += ` ORDER BY ${sortCol} ${sortD}`;
    const offset = (parseInt(page) - 1) * parseInt(limit);
    sql += ` LIMIT ${parseInt(limit)} OFFSET ${offset}`;

    const result = await dbQuery(sql, params);
    const items = formatResult(result);

    res.json({ items, total, page: parseInt(page), pages: Math.ceil(total / parseInt(limit)) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/movimentacoes', requireAuth, async (req, res) => {
  try {
    const { cliente_id, tipo_movimento, data_nf, duimp_di_processo, data_exoneracao, percentual, valor } = req.body;

    // Parceiro is always derived from the client's escritório (cadastro)
    const cliRes = await dbQuery("SELECT escritorio FROM clientes WHERE id = ?", [cliente_id]);
    const cliArr = formatResult(cliRes);
    const parceiro = cliArr.length > 0 ? (cliArr[0].escritorio || '') : '';

    // Calculate valor_ajustado automatically based on tipo_movimento
    let valor_ajustado = 0;
    if (tipo_movimento && tipo_movimento.includes('Débito')) {
      valor_ajustado = Math.abs(valor || 0) * -1;
    } else if (tipo_movimento && tipo_movimento.includes('Crédito')) {
      valor_ajustado = Math.abs(valor || 0);
    }

    await dbRun(`INSERT INTO movimentacoes (cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor, valor_ajustado)
      VALUES (?,?,?,?,?,?,?,?,?)`, [cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor || 0, valor_ajustado]);
    saveDb();
    await logAction(req.session.user.id, req.session.user.name, 'CREATE', 'movimentacao', null, `Lançamento criado para cliente ${cliente_id}`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put('/api/movimentacoes/:id', requireAuth, async (req, res) => {
  try {
    const { cliente_id, tipo_movimento, data_nf, duimp_di_processo, data_exoneracao, percentual, valor } = req.body;

    // Parceiro is always derived from the client's escritório (cadastro)
    const cliRes = await dbQuery("SELECT escritorio FROM clientes WHERE id = ?", [cliente_id]);
    const cliArr = formatResult(cliRes);
    const parceiro = cliArr.length > 0 ? (cliArr[0].escritorio || '') : '';

    // Calculate valor_ajustado automatically based on tipo_movimento
    let valor_ajustado = 0;
    if (tipo_movimento && tipo_movimento.includes('Débito')) {
      valor_ajustado = Math.abs(valor || 0) * -1;
    } else if (tipo_movimento && tipo_movimento.includes('Crédito')) {
      valor_ajustado = Math.abs(valor || 0);
    }

    await dbRun(`UPDATE movimentacoes SET cliente_id=?, tipo_movimento=?, data_nf=?, duimp_di_processo=?, parceiro=?, data_exoneracao=?, percentual=?, valor=?, valor_ajustado=?, updated_at=CURRENT_TIMESTAMP
      WHERE id=?`, [cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor, valor_ajustado, req.params.id]);
    saveDb();
    await logAction(req.session.user.id, req.session.user.name, 'UPDATE', 'movimentacao', req.params.id, `Lançamento atualizado`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/movimentacoes/:id', requireAuth, async (req, res) => {
  try {
    await dbRun("DELETE FROM movimentacoes WHERE id = ?", [req.params.id]);
    saveDb();
    await logAction(req.session.user.id, req.session.user.name, 'DELETE', 'movimentacao', req.params.id, `Lançamento excluído`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ SALDO / DASHBOARD ============
app.get('/api/saldos', requireAuth, async (req, res) => {
  try {
    const result = await dbQuery(`
      SELECT
        c.id, c.nome, c.escritorio, c.cliente_certificado,
        COALESCE(SUM(CASE WHEN m.tipo_movimento = 'Créditos Reconhecidos e Cedidos' THEN m.valor_ajustado ELSE 0 END), 0) as creditos,
        COALESCE(SUM(CASE WHEN m.tipo_movimento = 'Débitos de Liquidações' THEN m.valor_ajustado ELSE 0 END), 0) as debitos,
        COALESCE(SUM(CASE WHEN m.tipo_movimento = 'Débitos de Transferências' THEN m.valor_ajustado ELSE 0 END), 0) as transferencias,
        COUNT(CASE WHEN m.tipo_movimento = 'Débitos de Liquidações' THEN 1 END) as qtd_operacoes
      FROM clientes c
      LEFT JOIN movimentacoes m ON c.id = m.cliente_id
      GROUP BY c.id
      ORDER BY c.nome
    `);
    const items = formatResult(result);
    const saldos = items.map(item => {
      item.saldo = item.creditos + item.debitos + item.transferencias;
      item.media_operacao = item.qtd_operacoes > 0 ? Math.abs(item.debitos) / item.qtd_operacoes : 0;
      if (item.saldo < 0) item.situacao = 'Urgente - Comprar Saldo';
      else if (item.media_operacao > 0 && item.saldo < item.media_operacao * 2) item.situacao = 'Alerta - Comprar saldo';
      else item.situacao = 'Normal';
      return item;
    });
    res.json(saldos);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/dashboard', requireAuth, async (req, res) => {
  try {
    const clientes = await dbQuery("SELECT COUNT(*) as count FROM clientes");
    const movs = await dbQuery("SELECT COUNT(*) as count FROM movimentacoes");
    const creditos = await dbQuery("SELECT COALESCE(SUM(valor_ajustado),0) as total FROM movimentacoes WHERE tipo_movimento = 'Créditos Reconhecidos e Cedidos'");
    const debitos = await dbQuery("SELECT COALESCE(SUM(valor_ajustado),0) as total FROM movimentacoes WHERE tipo_movimento = 'Débitos de Liquidações'");
    const users = await dbQuery("SELECT COUNT(*) as count FROM users");

    const getCount = (result, field = 'count') => {
      if (isPostgres) {
        return result.rows[0]?.[field] || 0;
      } else {
        return result[0]?.values[0][0] || 0;
      }
    };

    const getTotal = (result, field = 'total') => {
      if (isPostgres) {
        return result.rows[0]?.[field] || 0;
      } else {
        return result[0]?.values[0][0] || 0;
      }
    };

    res.json({
      total_clientes: getCount(clientes),
      total_movimentacoes: getCount(movs),
      total_creditos: getTotal(creditos),
      total_debitos: getTotal(debitos),
      total_usuarios: getCount(users)
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ AUDIT LOG ============
app.get('/api/audit', requireAuth, requireAdmin, async (req, res) => {
  try {
    const result = await dbQuery("SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 100");
    const items = formatResult(result);
    res.json(items);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ RELATORIO / EXTRATO ============
app.get('/api/relatorio', requireAuth, async (req, res) => {
  try {
    const { cliente_id, data_inicio, data_fim } = req.query;
    let sql = `SELECT m.*, c.nome as cliente_nome FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id`;
    const params = [];
    const conditions = [];

    if (cliente_id) { conditions.push("m.cliente_id = ?"); params.push(cliente_id); }
    if (data_inicio) { conditions.push("m.data_nf >= ?"); params.push(data_inicio); }
    if (data_fim) { conditions.push("m.data_nf <= ?"); params.push(data_fim); }

    if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");
    sql += " ORDER BY m.data_nf ASC";

    const result = await dbQuery(sql, params);
    const items = formatResult(result).map(item => {
      delete item.parceiro;
      return item;
    });

    // Get all movements up to data_fim for saldo acumulado calculation
    let allMovementsUpToDataFim = [];
    if (cliente_id && data_fim) {
      const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.cliente_id = ? AND m.data_nf <= ? ORDER BY m.data_nf ASC`;
      const saldoResult = await dbQuery(saldoSql, [cliente_id, data_fim]);
      allMovementsUpToDataFim = formatResult(saldoResult);
    } else if (cliente_id) {
      const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.cliente_id = ? ORDER BY m.data_nf ASC`;
      const saldoResult = await dbQuery(saldoSql, [cliente_id]);
      allMovementsUpToDataFim = formatResult(saldoResult);
    } else if (data_fim) {
      const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.data_nf <= ? ORDER BY m.data_nf ASC`;
      const saldoResult = await dbQuery(saldoSql, [data_fim]);
      allMovementsUpToDataFim = formatResult(saldoResult);
    }

    // Calculate saldo acumulado from all movements up to data_fim
    let creditos = 0, debitos = 0, transferencias = 0;
    allMovementsUpToDataFim.forEach(m => {
      if (m.tipo_movimento === 'Créditos Reconhecidos e Cedidos') creditos += m.valor_ajustado || 0;
      else if (m.tipo_movimento === 'Débitos de Liquidações') debitos += m.valor_ajustado || 0;
      else if (m.tipo_movimento === 'Débitos de Transferências') transferencias += m.valor_ajustado || 0;
    });

    res.json({ items, resumo: { creditos, debitos, transferencias, saldo: creditos + debitos + transferencias } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ EXPORT EXCEL ============
app.get('/api/relatorio/excel', requireAuth, async (req, res) => {
  try {
    const { cliente_id, data_inicio, data_fim } = req.query;
    let sql = `SELECT c.nome as Cliente, m.tipo_movimento as "Tipo Movimento", m.data_nf as "Data NF", m.duimp_di_processo as "DUIMP/DI ou Processo", m.data_exoneracao as "Data Exoneração", m.percentual as "%", m.valor_ajustado as Valor FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id`;
    const params = [];
    const conditions = [];

    if (cliente_id) { conditions.push("m.cliente_id = ?"); params.push(cliente_id); }
    if (data_inicio) { conditions.push("m.data_nf >= ?"); params.push(data_inicio); }
    if (data_fim) { conditions.push("m.data_nf <= ?"); params.push(data_fim); }

    if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");
    sql += " ORDER BY m.data_nf ASC";

    const result = await dbQuery(sql, params);
    const rows = formatResult(result);

    // Get all movements up to data_fim for saldo acumulado calculation
    let creditos = 0, debitos = 0, transferencias = 0;
    if (cliente_id && data_fim) {
      const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.cliente_id = ? AND m.data_nf <= ?`;
      const saldoResult = await dbQuery(saldoSql, [cliente_id, data_fim]);
      formatResult(saldoResult).forEach(row => {
        if (row.tipo_movimento === 'Créditos Reconhecidos e Cedidos') creditos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Liquidações') debitos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Transferências') transferencias += row.valor_ajustado || 0;
      });
    } else if (cliente_id) {
      const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.cliente_id = ?`;
      const saldoResult = await dbQuery(saldoSql, [cliente_id]);
      formatResult(saldoResult).forEach(row => {
        if (row.tipo_movimento === 'Créditos Reconhecidos e Cedidos') creditos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Liquidações') debitos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Transferências') transferencias += row.valor_ajustado || 0;
      });
    } else if (data_fim) {
      const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.data_nf <= ?`;
      const saldoResult = await dbQuery(saldoSql, [data_fim]);
      formatResult(saldoResult).forEach(row => {
        if (row.tipo_movimento === 'Créditos Reconhecidos e Cedidos') creditos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Liquidações') debitos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Transferências') transferencias += row.valor_ajustado || 0;
      });
    }

    const wb = XLSX.utils.book_new();
    const header = ["Cliente", "Tipo Movimento", "Data NF", "DUIMP/DI ou Processo", "Data Exoneração", "%", "Valor"];

    let excelRows = [header];
    excelRows = excelRows.concat(rows.map(r => [r.Cliente, r['Tipo Movimento'], r['Data NF'], r['DUIMP/DI ou Processo'], r['Data Exoneração'], r['%'], r.Valor]));
    const ws = XLSX.utils.aoa_to_sheet(excelRows);

    // Column widths
    ws['!cols'] = [{ wch: 45 }, { wch: 35 }, { wch: 12 }, { wch: 20 }, { wch: 16 }, { wch: 6 }, { wch: 15 }];

    XLSX.utils.book_append_sheet(wb, ws, 'Extrato');

    // Summary sheet
    const wsResumo = XLSX.utils.aoa_to_sheet([
      ["RESUMO DO EXTRATO"],
      [],
      ["Total Créditos", creditos],
      ["Total Débitos", debitos],
      ["Total Transferências", transferencias],
      ["Saldo", creditos + debitos + transferencias]
    ]);
    wsResumo['!cols'] = [{ wch: 25 }, { wch: 20 }];
    XLSX.utils.book_append_sheet(wb, wsResumo, 'Resumo');

    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=extrato_conta_grafica.xlsx');
    res.send(Buffer.from(buf));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ EXPORT PDF (HTML-based) ============
app.get('/api/relatorio/pdf', requireAuth, async (req, res) => {
  try {
    const { cliente_id, data_inicio, data_fim } = req.query;
    let sql = `SELECT m.*, c.nome as cliente_nome FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id`;
    const params = [];
    const conditions = [];

    if (cliente_id) { conditions.push("m.cliente_id = ?"); params.push(cliente_id); }
    if (data_inicio) { conditions.push("m.data_nf >= ?"); params.push(data_inicio); }
    if (data_fim) { conditions.push("m.data_nf <= ?"); params.push(data_fim); }

    if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");
    sql += " ORDER BY m.data_nf ASC";

    const result = await dbQuery(sql, params);
    const items = formatResult(result);

    // Get all movements up to data_fim for saldo acumulado calculation
    let creditos = 0, debitos = 0, transferencias = 0;
    if (cliente_id && data_fim) {
      const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.cliente_id = ? AND m.data_nf <= ?`;
      const saldoResult = await dbQuery(saldoSql, [cliente_id, data_fim]);
      formatResult(saldoResult).forEach(row => {
        if (row.tipo_movimento === 'Créditos Reconhecidos e Cedidos') creditos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Liquidações') debitos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Transferências') transferencias += row.valor_ajustado || 0;
      });
    } else if (cliente_id) {
      const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.cliente_id = ?`;
      const saldoResult = await dbQuery(saldoSql, [cliente_id]);
      formatResult(saldoResult).forEach(row => {
        if (row.tipo_movimento === 'Créditos Reconhecidos e Cedidos') creditos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Liquidações') debitos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Transferências') transferencias += row.valor_ajustado || 0;
      });
    } else if (data_fim) {
      const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.data_nf <= ?`;
      const saldoResult = await dbQuery(saldoSql, [data_fim]);
      formatResult(saldoResult).forEach(row => {
        if (row.tipo_movimento === 'Créditos Reconhecidos e Cedidos') creditos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Liquidações') debitos += row.valor_ajustado || 0;
        else if (row.tipo_movimento === 'Débitos de Transferências') transferencias += row.valor_ajustado || 0;
      });
    }

    const saldo = creditos + debitos + transferencias;

    const clienteNome = items.length > 0 && cliente_id ? items[0].cliente_nome : 'Todos os Clientes';
    const periodo = (data_inicio || 'Início') + ' a ' + (data_fim || 'Atual');

    const fmtMoney = (v) => 'R$ ' + Number(v || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const fmtDate = (d) => { if (!d) return '-'; if (d.includes('-')) { const p = d.split('-'); return p[2]+'/'+p[1]+'/'+p[0]; } return d; };

    let rowsHtml = items.map(m => `<tr>
      <td>${m.cliente_nome || '-'}</td>
      <td>${m.tipo_movimento || '-'}</td>
      <td>${fmtDate(m.data_nf)}</td>
      <td>${m.duimp_di_processo || '-'}</td>
      <td style="text-align:right;">${fmtMoney(m.valor_ajustado)}</td>
    </tr>`).join('');

    const html = `<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Vision - Extrato de Conta Gráfica</title>
<style>
  @media print { @page { size: landscape; margin: 10mm; } body { -webkit-print-color-adjust: exact; } }
  body { font-family: Arial, sans-serif; font-size: 11px; color: #333; padding: 20px; }
  .header { text-align: center; margin-bottom: 20px; border-bottom: 2px solid #f47520; padding-bottom: 10px; }
  .header h1 { color: #f47520; font-size: 20px; margin: 0; }
  .header p { color: #666; margin: 4px 0 0; }
  .info { display: flex; justify-content: space-between; margin-bottom: 16px; }
  .info div { background: #f3f4f6; padding: 8px 12px; border-radius: 4px; }
  .resumo { display: flex; gap: 12px; margin-bottom: 20px; }
  .resumo .card { flex: 1; padding: 12px; border-radius: 6px; text-align: center; }
  .resumo .card.green { background: #d1fae5; color: #065f46; }
  .resumo .card.red { background: #fee2e2; color: #991b1b; }
  .resumo .card.blue { background: #dbeafe; color: #1e40af; }
  .resumo .card .lbl { font-size: 10px; text-transform: uppercase; }
  .resumo .card .val { font-size: 16px; font-weight: bold; margin-top: 4px; }
  table { width: 100%; border-collapse: collapse; margin-top: 10px; }
  th { background: #f47520; color: white; padding: 8px 6px; text-align: left; font-size: 10px; text-transform: uppercase; }
  td { padding: 6px; border-bottom: 1px solid #e5e7eb; font-size: 11px; }
  tr:nth-child(even) { background: #f9fafb; }
  .footer { margin-top: 20px; text-align: center; color: #999; font-size: 10px; border-top: 1px solid #ddd; padding-top: 8px; }
  .btn-print { background: #f47520; color: white; border: none; padding: 10px 24px; border-radius: 6px; cursor: pointer; font-size: 14px; margin-bottom: 16px; }
  .btn-print:hover { background: #d66a1b; }
  @media print { .no-print { display: none !important; } }
</style></head><body>
<div class="no-print" style="text-align:center;margin-bottom:16px;">
  <button class="btn-print" onclick="window.print()">Imprimir / Salvar PDF</button>
</div>
<div class="header">
  <h1>Vision - Extrato de Conta Gráfica</h1>
  <p>Saygo Group - Sistema de Gestão de Créditos</p>
</div>
<div class="info">
  <div><strong>Cliente:</strong> ${clienteNome}</div>
  <div><strong>Período:</strong> ${periodo}</div>
  <div><strong>Data emissão:</strong> ${new Date().toLocaleDateString('pt-BR')}</div>
</div>
<div class="resumo">
  <div class="card green"><div class="lbl">Créditos</div><div class="val">${fmtMoney(creditos)}</div></div>
  <div class="card red"><div class="lbl">Débitos</div><div class="val">${fmtMoney(debitos)}</div></div>
  <div class="card ${saldo >= 0 ? 'blue' : 'red'}"><div class="lbl">Saldo</div><div class="val">${fmtMoney(saldo)}</div></div>
</div>
<table><thead><tr><th>Cliente</th><th>Tipo Movimento</th><th>Data NF</th><th>DUIMP/DI</th><th>Valor</th></tr></thead>
<tbody>${rowsHtml}</tbody></table>
<div class="footer">Relatório gerado em ${new Date().toLocaleString('pt-BR')} — Sistema Conta Gráfica — Saygo Group</div>
</body></html>`;

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(html);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ ALERTAS SALDO BAIXO ============
app.get('/api/alertas', requireAuth, async (req, res) => {
  try {
    const result = await dbQuery(`
      SELECT
        c.id, c.nome, c.escritorio,
        COALESCE(SUM(CASE WHEN m.tipo_movimento = 'Créditos Reconhecidos e Cedidos' THEN m.valor_ajustado ELSE 0 END), 0) as creditos,
        COALESCE(SUM(CASE WHEN m.tipo_movimento = 'Débitos de Liquidações' THEN m.valor_ajustado ELSE 0 END), 0) as debitos,
        COALESCE(SUM(CASE WHEN m.tipo_movimento = 'Débitos de Transferências' THEN m.valor_ajustado ELSE 0 END), 0) as transferencias,
        COUNT(CASE WHEN m.tipo_movimento = 'Débitos de Liquidações' THEN 1 END) as qtd_operacoes
      FROM clientes c
      LEFT JOIN movimentacoes m ON c.id = m.cliente_id
      GROUP BY c.id
      ORDER BY c.nome
    `);
    const items = formatResult(result);
    const alertas = items.filter(obj => {
      obj.saldo = obj.creditos + obj.debitos + obj.transferencias;
      obj.media_operacao = obj.qtd_operacoes > 0 ? Math.abs(obj.debitos) / obj.qtd_operacoes : 0;
      if (obj.media_operacao > 0 && obj.saldo < obj.media_operacao) {
        obj.tipo = obj.saldo < 0 ? 'urgente' : 'alerta';
        return true;
      }
      return false;
    });
    res.json(alertas);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ IMPORT FROM XLSX ============
app.post('/api/import', requireAuth, requireAdmin, upload.single('file'), async (req, res) => {
  try {
    const filePath = req.file ? req.file.path : path.join(__dirname, '..', 'mnt', 'uploads', 'consolidado_conta_grafica (1)-2329fc78.xlsx');
    const workbook = XLSX.readFile(filePath);

    // Import Clientes
    const cadastroSheet = workbook.Sheets['Cadastro de Clientes'];
    if (cadastroSheet) {
      const data = XLSX.utils.sheet_to_json(cadastroSheet, { header: 1 });
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        if (!row[0]) continue;
        const existing = await dbQuery("SELECT id FROM clientes WHERE nome = ?", [row[0]]);
        const exists = isPostgres ? existing.rows.length > 0 : existing.length > 0 && existing[0].values.length > 0;
        if (!exists) {
          await dbRun(`INSERT INTO clientes (nome, escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [row[0], row[1] || '', row[2] || '', row[3] || '', row[4] || '', row[5] || '', row[6] || '', row[8] || '', row[9] || '', row[10] || '', row[7] || '']);
        }
      }
    }

    // Build client name -> id map
    const clientMap = {};
    const clientResult = await dbQuery("SELECT id, nome FROM clientes");
    const clients = formatResult(clientResult);
    clients.forEach(row => { clientMap[row.nome] = row.id; });

    // Import Consolidado
    const consolidadoSheet = workbook.Sheets['Consolidado'];
    if (consolidadoSheet) {
      const data = XLSX.utils.sheet_to_json(consolidadoSheet, { header: 1 });
      await dbRun("DELETE FROM movimentacoes"); // Clear existing
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        if (!row[0]) continue;
        const clienteId = clientMap[row[0]];
        if (!clienteId) continue;

        let dataNf = row[2];
        if (typeof dataNf === 'number') {
          const d = new Date((dataNf - 25569) * 86400 * 1000);
          dataNf = d.toISOString().split('T')[0];
        }
        let dataExo = row[5];
        if (typeof dataExo === 'number') {
          const d = new Date((dataExo - 25569) * 86400 * 1000);
          dataExo = d.toISOString().split('T')[0];
        }

        await dbRun(`INSERT INTO movimentacoes (cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor, valor_ajustado)
          VALUES (?,?,?,?,?,?,?,?,?)`, [clienteId, row[1] || '', dataNf || '', String(row[3] || ''), row[4] || '', dataExo || null, row[6] || null, row[7] || 0, row[8] || 0]);
      }
    }

    saveDb();
    await logAction(req.session.user.id, req.session.user.name, 'IMPORT', 'system', null, 'Importação de planilha realizada');
    res.json({ ok: true, message: 'Importação concluída com sucesso' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ IMPORT EXTRATO PDF ============

// Helpers for PDF extract parsing
function parseDateBR(s) {
  // '18/07/2025' -> '2025-07-18'
  const m = s && s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!m) return null;
  return `${m[3]}-${m[2]}-${m[1]}`;
}
function parseMoney(s) {
  if (!s) return 0;
  // 'R$ 1.260.000' / 'R$ 13.189,13' / 'R$ 1.195.568,9' / plain '1.260.000,00'
  let v = String(s).replace(/R\$\s*/ig, '').trim();
  v = v.replace(/\./g, '').replace(',', '.');
  const n = parseFloat(v);
  return isNaN(n) ? 0 : n;
}
function onlyDigits(s) { return (s || '').replace(/\D/g, ''); }

function parseExtratoPdf(text) {
  // Identify header
  const cnpjMatch = text.match(/CNPJ:\s*([\d./-]+)/i);
  const razaoMatch = text.match(/Raz[ãa]o\s+Social:\s*([^\n]+?)\s*(?:-\s*Ativa|\n|$)/i);
  const cnpj = cnpjMatch ? cnpjMatch[1].trim() : null;
  const razao_social = razaoMatch ? razaoMatch[1].trim() : null;

  const lines = text.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
  const movimentos = [];

  for (const line of lines) {
    // Skip obvious non-data lines
    if (/Gerado em:/i.test(line)) continue;
    if (/P[áa]gina\s+\d+\s+de\s+\d+/i.test(line)) continue;
    if (/Saldo\s+Total/i.test(line)) continue;
    if (/Total\s+de\s+(Cr[ée]ditos|D[ée]bitos)/i.test(line)) continue;
    if (/Cr[ée]ditos\s+Reconhecidos/i.test(line)) continue;
    if (/D[ée]bitos\s+de\s+Transfer[êe]ncias/i.test(line)) continue;
    if (/D[ée]bitos\s+de\s+Liquida[çc][õo]es/i.test(line)) continue;

    // Must start with date dd/mm/yyyy
    if (!/^\d{2}\/\d{2}\/\d{4}/.test(line)) continue;

    // Try LIQ first (most specific: two R$, middle is ICMS usually 0,00, then digit %, then valor)
    let m = line.match(/^(\d{2}\/\d{2}\/\d{4})\s*(.+?)\s*R\$\s*([\d.,]+)\s*(\d+)\s*R\$\s*([\d.,]+)\s*$/);
    if (m) {
      const valor = parseMoney('R$' + m[5]);
      movimentos.push({
        tipo_movimento: 'Débitos de Liquidações',
        data_nf: parseDateBR(m[1]),
        duimp_di_processo: m[2].replace(/\s+/g, ''),
        icms_devido: parseMoney('R$' + m[3]),
        percentual: parseFloat(m[4]),
        valor,
        valor_ajustado: -Math.abs(valor)
      });
      continue;
    }

    // Try CREDITO: has Natureza letter (single letter surrounded by whitespace) — T or A typically
    m = line.match(/^(\d{2}\/\d{2}\/\d{4})\s+(\d+)\s+([A-Z])\s*(?:(\d+)\s+)?R\$\s*([\d.,]+)(?:\s+(\d+))?\s*$/);
    if (m) {
      const valor = parseMoney('R$' + m[5]);
      const cg_debitada = m[4] || m[6] || '';
      movimentos.push({
        tipo_movimento: 'Créditos Reconhecidos e Cedidos',
        data_nf: parseDateBR(m[1]),
        duimp_di_processo: m[2],
        natureza: m[3],
        cg_debitada,
        valor,
        valor_ajustado: valor
      });
      continue;
    }

    // Try TRANS: date processo cg R$ valor (two numeric groups + 1 R$)
    m = line.match(/^(\d{2}\/\d{2}\/\d{4})\s+(\d+)\s+(\d+)\s+R\$\s*([\d.,]+)\s*$/);
    if (m) {
      const valor = parseMoney('R$' + m[4]);
      movimentos.push({
        tipo_movimento: 'Débitos de Transferências',
        data_nf: parseDateBR(m[1]),
        duimp_di_processo: m[2],
        cg_creditada: m[3],
        valor,
        valor_ajustado: -Math.abs(valor)
      });
      continue;
    }

    // Unmatched: line doesn't match any known row pattern — ignore silently
  }

  return { cnpj, razao_social, movimentos };
}

// Normalize keys for comparison
function normalizeDuimp(d) { return (d || '').toUpperCase().replace(/\s+/g, '').replace(/[^\w]/g, ''); }
function movKey(m) {
  return `${m.tipo_movimento}|${m.data_nf}|${normalizeDuimp(m.duimp_di_processo)}|${Number(m.valor_ajustado).toFixed(2)}`;
}

app.post('/api/import-extrato', requireAuth, requireAdmin, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Arquivo PDF não enviado' });
    const dataBuf = fs.readFileSync(req.file.path);
    const text = await extractPdfText(dataBuf);
    const parsed = parseExtratoPdf(text);

    if (!parsed.cnpj && !parsed.razao_social) {
      return res.status(400).json({ error: 'Não foi possível identificar o cliente no PDF (CNPJ / Razão Social não encontrados).' });
    }

    // Find client: try CNPJ first (normalized), then razão social (case-insensitive match)
    let cliente = null;
    const clientes = formatResult(await dbQuery("SELECT * FROM clientes"));
    if (parsed.cnpj) {
      const cnpjDigits = onlyDigits(parsed.cnpj);
      cliente = clientes.find(c => onlyDigits(c.cnpj) === cnpjDigits && cnpjDigits.length > 0) || null;
    }
    if (!cliente && parsed.razao_social) {
      const rsUp = parsed.razao_social.toUpperCase();
      cliente = clientes.find(c => (c.nome || '').toUpperCase() === rsUp) ||
                clientes.find(c => rsUp.includes((c.nome || '').toUpperCase()) || (c.nome || '').toUpperCase().includes(rsUp)) ||
                null;
    }

    if (!cliente) {
      return res.status(404).json({
        error: 'Cliente não encontrado no sistema.',
        cnpj: parsed.cnpj,
        razao_social: parsed.razao_social,
        movimentos_extraidos: parsed.movimentos.length
      });
    }

    // Load existing movimentacoes for this cliente
    const existRes = await dbQuery("SELECT * FROM movimentacoes WHERE cliente_id = ?", [cliente.id]);
    const existing = formatResult(existRes);

    // Build maps
    const extMovs = parsed.movimentos;
    const extByKey = {};
    extMovs.forEach(m => { extByKey[movKey(m)] = m; });
    const sysByKey = {};
    existing.forEach(m => { sysByKey[movKey(m)] = m; });

    // Matched: in both (same key)
    const matched = [];
    const missing_in_system = []; // in extract, not in system
    const missing_in_extract = []; // in system, not in extract
    const divergent = []; // same tipo + data + duimp but different valor

    // Helper to find by looser key
    const looseKey = (m) => `${m.tipo_movimento}|${m.data_nf}|${normalizeDuimp(m.duimp_di_processo)}`;
    const sysByLoose = {};
    existing.forEach(m => {
      const k = looseKey(m);
      if (!sysByLoose[k]) sysByLoose[k] = [];
      sysByLoose[k].push(m);
    });
    const extByLoose = {};
    extMovs.forEach(m => {
      const k = looseKey(m);
      if (!extByLoose[k]) extByLoose[k] = [];
      extByLoose[k].push(m);
    });

    const usedSysIds = new Set();

    for (const ext of extMovs) {
      const exactMatch = sysByKey[movKey(ext)];
      if (exactMatch) {
        matched.push({ extrato: ext, sistema: exactMatch });
        usedSysIds.add(exactMatch.id);
        continue;
      }
      // Look for loose match (same tipo+data+duimp) with different valor
      const candidates = (sysByLoose[looseKey(ext)] || []).filter(s => !usedSysIds.has(s.id));
      if (candidates.length > 0) {
        const sys = candidates[0];
        divergent.push({
          sistema: sys,
          extrato: ext,
          diferencas: {
            valor_sistema: sys.valor_ajustado,
            valor_extrato: ext.valor_ajustado
          }
        });
        usedSysIds.add(sys.id);
      } else {
        missing_in_system.push(ext);
      }
    }

    for (const sys of existing) {
      if (usedSysIds.has(sys.id)) continue;
      // If there's no exact match in extract
      if (!extByKey[movKey(sys)]) {
        // Also ensure it wasn't already counted as divergent
        missing_in_extract.push(sys);
      }
    }

    try { fs.unlinkSync(req.file.path); } catch (e) {}

    res.json({
      cliente: { id: cliente.id, nome: cliente.nome, cnpj: cliente.cnpj, escritorio: cliente.escritorio },
      extrato_header: { cnpj: parsed.cnpj, razao_social: parsed.razao_social },
      resumo: {
        total_extrato: extMovs.length,
        total_sistema: existing.length,
        matched: matched.length,
        divergent: divergent.length,
        missing_in_system: missing_in_system.length,
        missing_in_extract: missing_in_extract.length
      },
      matched,
      divergent,
      missing_in_system,
      missing_in_extract
    });
  } catch (e) {
    console.error('Erro na importação do extrato:', e);
    try { if (req.file) fs.unlinkSync(req.file.path); } catch (x) {}
    res.status(500).json({ error: e.message });
  }
});

// Apply actions from extrato comparison
app.post('/api/import-extrato/apply', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { cliente_id, includes = [], corrections = [], deletes = [] } = req.body;
    if (!cliente_id) return res.status(400).json({ error: 'cliente_id obrigatório' });

    // Get parceiro from client escritório
    const cliRes = await dbQuery("SELECT escritorio FROM clientes WHERE id = ?", [cliente_id]);
    const cliArr = formatResult(cliRes);
    const parceiro = cliArr.length > 0 ? (cliArr[0].escritorio || '') : '';

    let inc = 0, upd = 0, del = 0;

    // Include missing
    for (const m of includes) {
      const valor = Math.abs(Number(m.valor) || 0);
      const valor_ajustado = (m.tipo_movimento && m.tipo_movimento.includes('Débito')) ? -valor : valor;
      await dbRun(`INSERT INTO movimentacoes (cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor, valor_ajustado)
        VALUES (?,?,?,?,?,?,?,?,?)`,
        [cliente_id, m.tipo_movimento, m.data_nf, m.duimp_di_processo || '', parceiro, null, m.percentual || null, valor, valor_ajustado]);
      inc++;
    }

    // Correct divergent
    for (const c of corrections) {
      const id = c.id;
      const valor = Math.abs(Number(c.valor) || 0);
      const valor_ajustado = (c.tipo_movimento && c.tipo_movimento.includes('Débito')) ? -valor : valor;
      await dbRun(`UPDATE movimentacoes SET valor=?, valor_ajustado=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`,
        [valor, valor_ajustado, id]);
      upd++;
    }

    // Delete selected
    for (const id of deletes) {
      await dbRun(`DELETE FROM movimentacoes WHERE id = ?`, [id]);
      del++;
    }

    saveDb();
    await logAction(req.session.user.id, req.session.user.name, 'IMPORT', 'extrato', cliente_id,
      `Extrato importado: ${inc} incluídos, ${upd} corrigidos, ${del} excluídos`);
    res.json({ ok: true, incluidos: inc, corrigidos: upd, excluidos: del });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============ COMISSOES ============
app.get('/api/comissoes', requireAuth, async (req, res) => {
  try {
    const { parceiro, mes, ano } = req.query;

    // Get all clients with their commission settings
    const clientesResult = await dbQuery("SELECT id, nome, escritorio, percentual_comissao, dia_fechamento FROM clientes WHERE percentual_comissao > 0");
    const clientes = formatResult(clientesResult);
    if (clientes.length === 0) return res.json([]);

    // Get all credit movements
    const movsResult = await dbQuery("SELECT m.cliente_id, m.data_nf, m.valor_ajustado, m.tipo_movimento FROM movimentacoes m WHERE m.tipo_movimento = 'Créditos Reconhecidos e Cedidos' ORDER BY m.data_nf");
    const movimentos = formatResult(movsResult);

    // Determine date range: find min/max dates from movements
    if (movimentos.length === 0) return res.json([]);

    const allDates = movimentos.map(m => m.data_nf).filter(Boolean).sort();
    const minDate = new Date(allDates[0]);
    const maxDate = new Date(allDates[allDates.length - 1]);

    // Generate month/year periods from minDate to maxDate
    const comissoesPorParceiro = {};

    clientes.forEach(cliente => {
      const dia = cliente.dia_fechamento || 1;
      const pct = cliente.percentual_comissao || 0;
      if (pct <= 0) return;

      // Parceiro = campo escritório do cliente
      const parceiroNome = cliente.escritorio || 'Sem Escritório';

      // Filter movements for this client
      const clientMovs = movimentos.filter(m => m.cliente_id === cliente.id);
      if (clientMovs.length === 0) return;

      // For each month, calculate commission based on closing day
      let current = new Date(minDate.getFullYear(), minDate.getMonth(), 1);
      const end = new Date(maxDate.getFullYear(), maxDate.getMonth() + 1, 1);

      while (current <= end) {
        const year = current.getFullYear();
        const month = current.getMonth(); // 0-based

        // Period: from (dia+1) of previous month to (dia) of current month
        // Example: dia=22, month=Mar -> 23/Feb to 22/Mar
        const periodoInicio = new Date(year, month - 1, dia + 1);
        const periodoFim = new Date(year, month, dia);

        const inicioStr = periodoInicio.toISOString().split('T')[0];
        const fimStr = periodoFim.toISOString().split('T')[0];

        // Sum credits in the period
        const totalCreditos = clientMovs
          .filter(m => m.data_nf >= inicioStr && m.data_nf <= fimStr)
          .reduce((sum, m) => sum + (m.valor_ajustado || 0), 0);

        if (totalCreditos > 0) {
          const valorComissao = totalCreditos * (pct / 100);
          const mesAno = `${String(month + 1).padStart(2, '0')}/${year}`;
          const p = parceiroNome;

          if (!parceiro || p === parceiro) {
            const key = `${p}|${mesAno}`;
            if (!comissoesPorParceiro[key]) {
              comissoesPorParceiro[key] = {
                parceiro: p,
                mes_ano: mesAno,
                total_comissao: 0,
                detalhes: []
              };
            }
            comissoesPorParceiro[key].total_comissao += valorComissao;
            comissoesPorParceiro[key].detalhes.push({
              cliente_id: cliente.id,
              cliente_nome: cliente.nome,
              total_creditos: totalCreditos,
              percentual: pct,
              valor_comissao: valorComissao,
              periodo_inicio: inicioStr,
              periodo_fim: fimStr
            });
          }
        }

        current.setMonth(current.getMonth() + 1);
      }
    });

    // Convert to array and sort by date desc
    let result = Object.values(comissoesPorParceiro).sort((a, b) => {
      const [mA, yA] = a.mes_ano.split('/');
      const [mB, yB] = b.mes_ano.split('/');
      return (yB + mB).localeCompare(yA + mA) || a.parceiro.localeCompare(b.parceiro);
    });

    // Filter by mes/ano if provided
    if (mes || ano) {
      result = result.filter(r => {
        const [m, y] = r.mes_ano.split('/');
        if (mes && ano) return m === mes && y === ano;
        if (mes) return m === mes;
        if (ano) return y === ano;
        return true;
      });
    }

    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Start server
async function start() {
  try {
    // Check if DATABASE_URL is set for PostgreSQL
    if (process.env.DATABASE_URL) {
      isPostgres = true;
      pgPool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.DATABASE_SSL === 'true' ? { rejectUnauthorized: false } : false
      });

      // Test connection
      const conn = await pgPool.connect();
      conn.release();
      console.log('Conectado ao PostgreSQL com DATABASE_URL');
    } else {
      // Fall back to sql.js
      isPostgres = false;
      const SQL = await initSqlJs();
      if (fs.existsSync(DB_PATH)) {
        const fileBuffer = fs.readFileSync(DB_PATH);
        db = new SQL.Database(fileBuffer);
      } else {
        db = new SQL.Database();
      }
      console.log('Usando sql.js (SQLite) em memória');
    }

    // Initialize database schema and default users
    await initDatabase();

    // JSON error handler for /api/* routes (keep before listen)
    app.use('/api', (err, req, res, next) => {
      console.error('API error:', err);
      if (res.headersSent) return next(err);
      res.status(500).json({ error: err.message || 'Erro interno do servidor' });
    });
    // JSON 404 for unknown API routes
    app.use('/api', (req, res) => {
      res.status(404).json({ error: 'Rota não encontrada' });
    });

    app.listen(PORT, '0.0.0.0', () => {
      console.log(`Sistema Conta Gráfica rodando em http://localhost:${PORT}`);
      if (isPostgres) {
        console.log('Modo: PostgreSQL via DATABASE_URL');
      } else {
        console.log('Modo: SQLite (sql.js) - dados locais');
      }
    });
  } catch (error) {
    console.error('Erro ao inicializar aplicação:', error);
    process.exit(1);
  }
}

start();
