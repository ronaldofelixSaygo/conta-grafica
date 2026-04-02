const express = require('express');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const initSqlJs = require('sql.js');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const XLSX = require('xlsx');

const app = express();
const PORT = process.env.PORT || 3000;
const DB_PATH = path.join(__dirname, 'database.sqlite');

let db;

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));
app.use(session({
  secret: process.env.SESSION_SECRET || 'conta-grafica-secret-key-2026',
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 24 * 60 * 60 * 1000 }
}));

const upload = multer({ dest: 'uploads/' });

function saveDb() {
  const data = db.export();
  const buffer = Buffer.from(data);
  fs.writeFileSync(DB_PATH, buffer);
}

function initDatabase() {
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
    escritorio TEXT,
    locacao_sala TEXT DEFAULT 'Não',
    abertura_filial TEXT DEFAULT 'Não',
    reativacao_ie TEXT DEFAULT 'Não',
    conta_grafica TEXT DEFAULT 'Não',
    cliente_certificado TEXT DEFAULT 'Não',
    parceiro_sala TEXT,
    parceiro_filial TEXT,
    parceiro_ie TEXT,
    observacoes TEXT,
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

  // Create default admin user
  const admin = db.exec("SELECT id FROM users WHERE email = 'admin@saygogroup.com.br'");
  if (admin.length === 0) {
    const hash = bcrypt.hashSync('admin123', 10);
    db.run("INSERT INTO users (name, email, password, role) VALUES (?, ?, ?, ?)",
      ['Administrador', 'admin@saygogroup.com.br', hash, 'admin']);
  }

  // Create Ronaldo user
  const ronaldo = db.exec("SELECT id FROM users WHERE email = 'ronaldo.felix@saygogroup.com.br'");
  if (ronaldo.length === 0) {
    const hash = bcrypt.hashSync('123456', 10);
    db.run("INSERT INTO users (name, email, password, role) VALUES (?, ?, ?, ?)",
      ['Ronaldo Felix', 'ronaldo.felix@saygogroup.com.br', hash, 'admin']);
  }

  saveDb();
}

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

function logAction(userId, userName, action, entity, entityId, details) {
  db.run("INSERT INTO audit_log (user_id, user_name, action, entity, entity_id, details) VALUES (?,?,?,?,?,?)",
    [userId, userName, action, entity, entityId, details]);
  saveDb();
}

// ============ AUTH ROUTES ============
app.post('/api/auth/login', (req, res) => {
  const { email, password } = req.body;
  const result = db.exec("SELECT * FROM users WHERE email = ?", [email]);
  if (result.length === 0 || result[0].values.length === 0)
    return res.status(401).json({ error: 'Email ou senha inválidos' });

  const cols = result[0].columns;
  const row = result[0].values[0];
  const user = {};
  cols.forEach((c, i) => user[c] = row[i]);

  if (!bcrypt.compareSync(password, user.password))
    return res.status(401).json({ error: 'Email ou senha inválidos' });

  req.session.user = { id: user.id, name: user.name, email: user.email, role: user.role };
  res.json({ user: req.session.user });
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
app.get('/api/users', requireAuth, requireAdmin, (req, res) => {
  const result = db.exec("SELECT id, name, email, role, created_at FROM users ORDER BY name");
  if (result.length === 0) return res.json([]);
  const cols = result[0].columns;
  const users = result[0].values.map(row => {
    const obj = {};
    cols.forEach((c, i) => obj[c] = row[i]);
    return obj;
  });
  res.json(users);
});

app.post('/api/users', requireAuth, requireAdmin, (req, res) => {
  const { name, email, password, role } = req.body;
  try {
    const hash = bcrypt.hashSync(password, 10);
    db.run("INSERT INTO users (name, email, password, role) VALUES (?,?,?,?)", [name, email, hash, role || 'user']);
    saveDb();
    logAction(req.session.user.id, req.session.user.name, 'CREATE', 'user', null, `Usuário criado: ${name}`);
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: 'Email já cadastrado' });
  }
});

app.delete('/api/users/:id', requireAuth, requireAdmin, (req, res) => {
  db.run("DELETE FROM users WHERE id = ? AND id != ?", [req.params.id, req.session.user.id]);
  saveDb();
  res.json({ ok: true });
});

// ============ CLIENTES ROUTES ============
app.get('/api/clientes', requireAuth, (req, res) => {
  const result = db.exec("SELECT * FROM clientes ORDER BY nome");
  if (result.length === 0) return res.json([]);
  const cols = result[0].columns;
  const items = result[0].values.map(row => {
    const obj = {};
    cols.forEach((c, i) => obj[c] = row[i]);
    return obj;
  });
  res.json(items);
});

app.post('/api/clientes', requireAuth, (req, res) => {
  const { nome, escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes } = req.body;
  db.run(`INSERT INTO clientes (nome, escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [nome, escritorio, locacao_sala || 'Não', abertura_filial || 'Não', reativacao_ie || 'Não', conta_grafica || 'Não', cliente_certificado || 'Não', parceiro_sala || '', parceiro_filial || '', parceiro_ie || '', observacoes || '']);
  saveDb();
  logAction(req.session.user.id, req.session.user.name, 'CREATE', 'cliente', null, `Cliente criado: ${nome}`);
  res.json({ ok: true });
});

app.put('/api/clientes/:id', requireAuth, (req, res) => {
  const { nome, escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes } = req.body;
  db.run(`UPDATE clientes SET nome=?, escritorio=?, locacao_sala=?, abertura_filial=?, reativacao_ie=?, conta_grafica=?, cliente_certificado=?, parceiro_sala=?, parceiro_filial=?, parceiro_ie=?, observacoes=?, updated_at=CURRENT_TIMESTAMP
    WHERE id=?`, [nome, escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes, req.params.id]);
  saveDb();
  logAction(req.session.user.id, req.session.user.name, 'UPDATE', 'cliente', req.params.id, `Cliente atualizado: ${nome}`);
  res.json({ ok: true });
});

app.delete('/api/clientes/:id', requireAuth, (req, res) => {
  const result = db.exec("SELECT nome FROM clientes WHERE id = ?", [req.params.id]);
  const nome = result.length > 0 ? result[0].values[0][0] : 'N/A';
  db.run("DELETE FROM movimentacoes WHERE cliente_id = ?", [req.params.id]);
  db.run("DELETE FROM clientes WHERE id = ?", [req.params.id]);
  saveDb();
  logAction(req.session.user.id, req.session.user.name, 'DELETE', 'cliente', req.params.id, `Cliente excluído: ${nome}`);
  res.json({ ok: true });
});

// ============ MOVIMENTACOES ROUTES ============
app.get('/api/movimentacoes', requireAuth, (req, res) => {
  const { cliente_id, page = 1, limit = 50, search } = req.query;
  let sql = `SELECT m.*, c.nome as cliente_nome FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id`;
  const params = [];
  const conditions = [];

  if (cliente_id) { conditions.push("m.cliente_id = ?"); params.push(cliente_id); }
  if (search) { conditions.push("(c.nome LIKE ? OR m.duimp_di_processo LIKE ?)"); params.push(`%${search}%`, `%${search}%`); }

  if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");

  // Count total
  const countSql = sql.replace("m.*, c.nome as cliente_nome", "COUNT(*) as total");
  const countResult = db.exec(countSql, params);
  const total = countResult.length > 0 ? countResult[0].values[0][0] : 0;

  sql += " ORDER BY m.data_nf DESC";
  const offset = (parseInt(page) - 1) * parseInt(limit);
  sql += ` LIMIT ${parseInt(limit)} OFFSET ${offset}`;

  const result = db.exec(sql, params);
  if (result.length === 0) return res.json({ items: [], total, page: parseInt(page), pages: Math.ceil(total / parseInt(limit)) });

  const cols = result[0].columns;
  const items = result[0].values.map(row => {
    const obj = {};
    cols.forEach((c, i) => obj[c] = row[i]);
    return obj;
  });
  res.json({ items, total, page: parseInt(page), pages: Math.ceil(total / parseInt(limit)) });
});

app.post('/api/movimentacoes', requireAuth, (req, res) => {
  const { cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor } = req.body;

  // Calculate valor_ajustado automatically based on tipo_movimento
  let valor_ajustado = 0;
  if (tipo_movimento && tipo_movimento.includes('Débito')) {
    valor_ajustado = Math.abs(valor || 0) * -1;
  } else if (tipo_movimento && tipo_movimento.includes('Crédito')) {
    valor_ajustado = Math.abs(valor || 0);
  }

  db.run(`INSERT INTO movimentacoes (cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor, valor_ajustado)
    VALUES (?,?,?,?,?,?,?,?,?)`, [cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor || 0, valor_ajustado]);
  saveDb();
  logAction(req.session.user.id, req.session.user.name, 'CREATE', 'movimentacao', null, `Lançamento criado para cliente ${cliente_id}`);
  res.json({ ok: true });
});

app.put('/api/movimentacoes/:id', requireAuth, (req, res) => {
  const { cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor } = req.body;

  // Calculate valor_ajustado automatically based on tipo_movimento
  let valor_ajustado = 0;
  if (tipo_movimento && tipo_movimento.includes('Débito')) {
    valor_ajustado = Math.abs(valor || 0) * -1;
  } else if (tipo_movimento && tipo_movimento.includes('Crédito')) {
    valor_ajustado = Math.abs(valor || 0);
  }

  db.run(`UPDATE movimentacoes SET cliente_id=?, tipo_movimento=?, data_nf=?, duimp_di_processo=?, parceiro=?, data_exoneracao=?, percentual=?, valor=?, valor_ajustado=?, updated_at=CURRENT_TIMESTAMP
    WHERE id=?`, [cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor, valor_ajustado, req.params.id]);
  saveDb();
  logAction(req.session.user.id, req.session.user.name, 'UPDATE', 'movimentacao', req.params.id, `Lançamento atualizado`);
  res.json({ ok: true });
});

app.delete('/api/movimentacoes/:id', requireAuth, (req, res) => {
  db.run("DELETE FROM movimentacoes WHERE id = ?", [req.params.id]);
  saveDb();
  logAction(req.session.user.id, req.session.user.name, 'DELETE', 'movimentacao', req.params.id, `Lançamento excluído`);
  res.json({ ok: true });
});

// ============ SALDO / DASHBOARD ============
app.get('/api/saldos', requireAuth, (req, res) => {
  const result = db.exec(`
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
  if (result.length === 0) return res.json([]);
  const cols = result[0].columns;
  const items = result[0].values.map(row => {
    const obj = {};
    cols.forEach((c, i) => obj[c] = row[i]);
    obj.saldo = obj.creditos + obj.debitos + obj.transferencias;
    obj.media_operacao = obj.qtd_operacoes > 0 ? Math.abs(obj.debitos) / obj.qtd_operacoes : 0;
    if (obj.saldo < 0) obj.situacao = 'Urgente - Comprar Saldo';
    else if (obj.media_operacao > 0 && obj.saldo < obj.media_operacao * 2) obj.situacao = 'Alerta - Comprar saldo';
    else obj.situacao = 'Normal';
    return obj;
  });
  res.json(items);
});

app.get('/api/dashboard', requireAuth, (req, res) => {
  const clientes = db.exec("SELECT COUNT(*) FROM clientes");
  const movs = db.exec("SELECT COUNT(*) FROM movimentacoes");
  const creditos = db.exec("SELECT COALESCE(SUM(valor_ajustado),0) FROM movimentacoes WHERE tipo_movimento = 'Créditos Reconhecidos e Cedidos'");
  const debitos = db.exec("SELECT COALESCE(SUM(valor_ajustado),0) FROM movimentacoes WHERE tipo_movimento = 'Débitos de Liquidações'");
  const users = db.exec("SELECT COUNT(*) FROM users");

  res.json({
    total_clientes: clientes[0]?.values[0][0] || 0,
    total_movimentacoes: movs[0]?.values[0][0] || 0,
    total_creditos: creditos[0]?.values[0][0] || 0,
    total_debitos: debitos[0]?.values[0][0] || 0,
    total_usuarios: users[0]?.values[0][0] || 0
  });
});

// ============ AUDIT LOG ============
app.get('/api/audit', requireAuth, requireAdmin, (req, res) => {
  const result = db.exec("SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 100");
  if (result.length === 0) return res.json([]);
  const cols = result[0].columns;
  const items = result[0].values.map(row => {
    const obj = {};
    cols.forEach((c, i) => obj[c] = row[i]);
    return obj;
  });
  res.json(items);
});

// ============ RELATORIO / EXTRATO ============
app.get('/api/relatorio', requireAuth, (req, res) => {
  const { cliente_id, data_inicio, data_fim } = req.query;
  let sql = `SELECT m.*, c.nome as cliente_nome FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id`;
  const params = [];
  const conditions = [];

  if (cliente_id) { conditions.push("m.cliente_id = ?"); params.push(cliente_id); }
  if (data_inicio) { conditions.push("m.data_nf >= ?"); params.push(data_inicio); }
  if (data_fim) { conditions.push("m.data_nf <= ?"); params.push(data_fim); }

  if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");
  sql += " ORDER BY m.data_nf ASC";

  const result = db.exec(sql, params);

  // Get all movements up to data_fim for saldo acumulado calculation
  let allMovementsUpToDataFim = [];
  if (cliente_id && data_fim) {
    const saldoSql = `SELECT m.*, c.nome as cliente_nome FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id WHERE m.cliente_id = ? AND m.data_nf <= ? ORDER BY m.data_nf ASC`;
    const saldoResult = db.exec(saldoSql, [cliente_id, data_fim]);
    if (saldoResult.length > 0) {
      allMovementsUpToDataFim = saldoResult[0].values.map(row => {
        const obj = {};
        saldoResult[0].columns.forEach((c, i) => obj[c] = row[i]);
        return obj;
      });
    }
  } else if (cliente_id) {
    const saldoSql = `SELECT m.*, c.nome as cliente_nome FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id WHERE m.cliente_id = ? ORDER BY m.data_nf ASC`;
    const saldoResult = db.exec(saldoSql, [cliente_id]);
    if (saldoResult.length > 0) {
      allMovementsUpToDataFim = saldoResult[0].values.map(row => {
        const obj = {};
        saldoResult[0].columns.forEach((c, i) => obj[c] = row[i]);
        return obj;
      });
    }
  } else if (data_fim) {
    const saldoSql = `SELECT m.*, c.nome as cliente_nome FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id WHERE m.data_nf <= ? ORDER BY m.data_nf ASC`;
    const saldoResult = db.exec(saldoSql, [data_fim]);
    if (saldoResult.length > 0) {
      allMovementsUpToDataFim = saldoResult[0].values.map(row => {
        const obj = {};
        saldoResult[0].columns.forEach((c, i) => obj[c] = row[i]);
        return obj;
      });
    }
  }

  if (result.length === 0) return res.json({ items: [], resumo: { creditos: 0, debitos: 0, transferencias: 0, saldo: 0 } });

  const cols = result[0].columns;
  const items = result[0].values.map(row => {
    const obj = {};
    cols.forEach((c, i) => obj[c] = row[i]);
    // Remove parceiro from response items
    delete obj.parceiro;
    return obj;
  });

  // Calculate saldo acumulado from all movements up to data_fim
  let creditos = 0, debitos = 0, transferencias = 0;
  allMovementsUpToDataFim.forEach(m => {
    if (m.tipo_movimento === 'Créditos Reconhecidos e Cedidos') creditos += m.valor_ajustado || 0;
    else if (m.tipo_movimento === 'Débitos de Liquidações') debitos += m.valor_ajustado || 0;
    else if (m.tipo_movimento === 'Débitos de Transferências') transferencias += m.valor_ajustado || 0;
  });

  res.json({ items, resumo: { creditos, debitos, transferencias, saldo: creditos + debitos + transferencias } });
});

// ============ EXPORT EXCEL ============
app.get('/api/relatorio/excel', requireAuth, (req, res) => {
  const { cliente_id, data_inicio, data_fim } = req.query;
  let sql = `SELECT c.nome as Cliente, m.tipo_movimento as "Tipo Movimento", m.data_nf as "Data NF", m.duimp_di_processo as "DUIMP/DI ou Processo", m.data_exoneracao as "Data Exoneração", m.percentual as "%", m.valor_ajustado as Valor FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id`;
  const params = [];
  const conditions = [];

  if (cliente_id) { conditions.push("m.cliente_id = ?"); params.push(cliente_id); }
  if (data_inicio) { conditions.push("m.data_nf >= ?"); params.push(data_inicio); }
  if (data_fim) { conditions.push("m.data_nf <= ?"); params.push(data_fim); }

  if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");
  sql += " ORDER BY m.data_nf ASC";

  const result = db.exec(sql, params);

  // Get all movements up to data_fim for saldo acumulado calculation
  let creditos = 0, debitos = 0, transferencias = 0;
  if (cliente_id && data_fim) {
    const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.cliente_id = ? AND m.data_nf <= ?`;
    const saldoResult = db.exec(saldoSql, [cliente_id, data_fim]);
    if (saldoResult.length > 0) {
      saldoResult[0].values.forEach(row => {
        const tipo = row[saldoResult[0].columns.indexOf('tipo_movimento')];
        const val = row[saldoResult[0].columns.indexOf('valor_ajustado')] || 0;
        if (tipo === 'Créditos Reconhecidos e Cedidos') creditos += val;
        else if (tipo === 'Débitos de Liquidações') debitos += val;
        else if (tipo === 'Débitos de Transferências') transferencias += val;
      });
    }
  } else if (cliente_id) {
    const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.cliente_id = ?`;
    const saldoResult = db.exec(saldoSql, [cliente_id]);
    if (saldoResult.length > 0) {
      saldoResult[0].values.forEach(row => {
        const tipo = row[saldoResult[0].columns.indexOf('tipo_movimento')];
        const val = row[saldoResult[0].columns.indexOf('valor_ajustado')] || 0;
        if (tipo === 'Créditos Reconhecidos e Cedidos') creditos += val;
        else if (tipo === 'Débitos de Liquidações') debitos += val;
        else if (tipo === 'Débitos de Transferências') transferencias += val;
      });
    }
  } else if (data_fim) {
    const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.data_nf <= ?`;
    const saldoResult = db.exec(saldoSql, [data_fim]);
    if (saldoResult.length > 0) {
      saldoResult[0].values.forEach(row => {
        const tipo = row[saldoResult[0].columns.indexOf('tipo_movimento')];
        const val = row[saldoResult[0].columns.indexOf('valor_ajustado')] || 0;
        if (tipo === 'Créditos Reconhecidos e Cedidos') creditos += val;
        else if (tipo === 'Débitos de Liquidações') debitos += val;
        else if (tipo === 'Débitos de Transferências') transferencias += val;
      });
    }
  }

  const wb = XLSX.utils.book_new();
  const header = ["Cliente", "Tipo Movimento", "Data NF", "DUIMP/DI ou Processo", "Data Exoneração", "%", "Valor"];

  let rows = [header];
  if (result.length > 0) {
    rows = rows.concat(result[0].values);
  }
  const ws = XLSX.utils.aoa_to_sheet(rows);

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
});

// ============ EXPORT PDF (HTML-based) ============
app.get('/api/relatorio/pdf', requireAuth, (req, res) => {
  const { cliente_id, data_inicio, data_fim } = req.query;
  let sql = `SELECT m.*, c.nome as cliente_nome FROM movimentacoes m LEFT JOIN clientes c ON m.cliente_id = c.id`;
  const params = [];
  const conditions = [];

  if (cliente_id) { conditions.push("m.cliente_id = ?"); params.push(cliente_id); }
  if (data_inicio) { conditions.push("m.data_nf >= ?"); params.push(data_inicio); }
  if (data_fim) { conditions.push("m.data_nf <= ?"); params.push(data_fim); }

  if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");
  sql += " ORDER BY m.data_nf ASC";

  const result = db.exec(sql, params);
  const items = result.length > 0 ? result[0].values.map(row => {
    const obj = {};
    result[0].columns.forEach((c, i) => obj[c] = row[i]);
    return obj;
  }) : [];

  // Get all movements up to data_fim for saldo acumulado calculation
  let creditos = 0, debitos = 0, transferencias = 0;
  if (cliente_id && data_fim) {
    const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.cliente_id = ? AND m.data_nf <= ?`;
    const saldoResult = db.exec(saldoSql, [cliente_id, data_fim]);
    if (saldoResult.length > 0) {
      saldoResult[0].values.forEach(row => {
        const tipo = row[saldoResult[0].columns.indexOf('tipo_movimento')];
        const val = row[saldoResult[0].columns.indexOf('valor_ajustado')] || 0;
        if (tipo === 'Créditos Reconhecidos e Cedidos') creditos += val;
        else if (tipo === 'Débitos de Liquidações') debitos += val;
        else if (tipo === 'Débitos de Transferências') transferencias += val;
      });
    }
  } else if (cliente_id) {
    const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.cliente_id = ?`;
    const saldoResult = db.exec(saldoSql, [cliente_id]);
    if (saldoResult.length > 0) {
      saldoResult[0].values.forEach(row => {
        const tipo = row[saldoResult[0].columns.indexOf('tipo_movimento')];
        const val = row[saldoResult[0].columns.indexOf('valor_ajustado')] || 0;
        if (tipo === 'Créditos Reconhecidos e Cedidos') creditos += val;
        else if (tipo === 'Débitos de Liquidações') debitos += val;
        else if (tipo === 'Débitos de Transferências') transferencias += val;
      });
    }
  } else if (data_fim) {
    const saldoSql = `SELECT m.* FROM movimentacoes m WHERE m.data_nf <= ?`;
    const saldoResult = db.exec(saldoSql, [data_fim]);
    if (saldoResult.length > 0) {
      saldoResult[0].values.forEach(row => {
        const tipo = row[saldoResult[0].columns.indexOf('tipo_movimento')];
        const val = row[saldoResult[0].columns.indexOf('valor_ajustado')] || 0;
        if (tipo === 'Créditos Reconhecidos e Cedidos') creditos += val;
        else if (tipo === 'Débitos de Liquidações') debitos += val;
        else if (tipo === 'Débitos de Transferências') transferencias += val;
      });
    }
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
});

// ============ ALERTAS SALDO BAIXO ============
app.get('/api/alertas', requireAuth, (req, res) => {
  const result = db.exec(`
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
  if (result.length === 0) return res.json([]);
  const cols = result[0].columns;
  const alertas = [];
  result[0].values.forEach(row => {
    const obj = {};
    cols.forEach((c, i) => obj[c] = row[i]);
    obj.saldo = obj.creditos + obj.debitos + obj.transferencias;
    obj.media_operacao = obj.qtd_operacoes > 0 ? Math.abs(obj.debitos) / obj.qtd_operacoes : 0;
    if (obj.media_operacao > 0 && obj.saldo < obj.media_operacao) {
      obj.tipo = obj.saldo < 0 ? 'urgente' : 'alerta';
      alertas.push(obj);
    }
  });
  res.json(alertas);
});

// ============ IMPORT FROM XLSX ============
app.post('/api/import', requireAuth, requireAdmin, upload.single('file'), (req, res) => {
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
        const existing = db.exec("SELECT id FROM clientes WHERE nome = ?", [row[0]]);
        if (existing.length === 0 || existing[0].values.length === 0) {
          db.run(`INSERT INTO clientes (nome, escritorio, locacao_sala, abertura_filial, reativacao_ie, conta_grafica, cliente_certificado, parceiro_sala, parceiro_filial, parceiro_ie, observacoes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)`, [row[0], row[1] || '', row[2] || 'Não', row[3] || 'Não', row[4] || 'Não', row[5] || 'Não', row[6] || 'Não', row[8] || '', row[9] || '', row[10] || '', row[7] || '']);
        }
      }
    }

    // Build client name -> id map
    const clientMap = {};
    const clientResult = db.exec("SELECT id, nome FROM clientes");
    if (clientResult.length > 0) {
      clientResult[0].values.forEach(row => { clientMap[row[1]] = row[0]; });
    }

    // Import Consolidado
    const consolidadoSheet = workbook.Sheets['Consolidado'];
    if (consolidadoSheet) {
      const data = XLSX.utils.sheet_to_json(consolidadoSheet, { header: 1 });
      db.run("DELETE FROM movimentacoes"); // Clear existing
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

        db.run(`INSERT INTO movimentacoes (cliente_id, tipo_movimento, data_nf, duimp_di_processo, parceiro, data_exoneracao, percentual, valor, valor_ajustado)
          VALUES (?,?,?,?,?,?,?,?,?)`, [clienteId, row[1] || '', dataNf || '', String(row[3] || ''), row[4] || '', dataExo || null, row[6] || null, row[7] || 0, row[8] || 0]);
      }
    }

    saveDb();
    logAction(req.session.user.id, req.session.user.name, 'IMPORT', 'system', null, 'Importação de planilha realizada');
    res.json({ ok: true, message: 'Importação concluída com sucesso' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Start server
async function start() {
  const SQL = await initSqlJs();
  if (fs.existsSync(DB_PATH)) {
    const fileBuffer = fs.readFileSync(DB_PATH);
    db = new SQL.Database(fileBuffer);
  } else {
    db = new SQL.Database();
  }
  initDatabase();

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Sistema Conta Gráfica rodando em http://localhost:${PORT}`);
  });
}

start();
