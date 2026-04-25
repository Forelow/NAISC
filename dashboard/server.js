require('dotenv').config();
const express = require('express');
const OpenAI = require('openai');
const mysql = require('mysql2/promise');

const app = express();
const PORT = Number(process.env.PORT || 3000);
const MAX_ROWS_PER_TABLE = 1000;

const pool = mysql.createPool({
    host: process.env.MYSQL_HOST || '127.0.0.1',
    port: Number(process.env.MYSQL_PORT || 3306),
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || '',
    database: process.env.MYSQL_DATABASE || 'semicon_parser',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

const openai = process.env.OPENAI_API_KEY
    ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
    : null;

app.set('view engine', 'ejs');
app.set('views', __dirname + '/views');
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

async function query(sql, params = []) {
    const [rows] = await pool.query(sql, params);
    return rows;
}

function numericOrNull(value) {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function normalizeRows(tableName, rows) {
    if (tableName === 'sensor_readings') {
        return rows.map(r => ({
            ...r,
            value: numericOrNull(r.value)
        }));
    }

    if (tableName === 'process_parameters_recipes') {
        return rows.map(r => ({
            ...r,
            value: numericOrNull(r.value)
        }));
    }

    if (tableName === 'wafer_processing_sequences') {
        return rows.map(r => ({
            ...r,
            duration_s: numericOrNull(r.duration_s),
            wafer_count: numericOrNull(r.wafer_count)
        }));
    }

    return rows;
}

async function loadDashboardData() {
    const summary = {};

    const [filesCount] = await query('SELECT COUNT(*) AS count FROM files');
    const [eqCount] = await query('SELECT COUNT(*) AS count FROM equipment_states');
    const [procCount] = await query('SELECT COUNT(*) AS count FROM process_parameters_recipes');
    const [sensorCount] = await query('SELECT COUNT(*) AS count FROM sensor_readings');
    const [faultCount] = await query('SELECT COUNT(*) AS count FROM fault_events');
    const [waferCount] = await query('SELECT COUNT(*) AS count FROM wafer_processing_sequences');
    const [rejectCount] = await query('SELECT COUNT(*) AS count FROM rejected_records');

    summary.files = filesCount?.count || 0;
    summary.equipment_states = eqCount?.count || 0;
    summary.process_parameters_recipes = procCount?.count || 0;
    summary.sensor_readings = sensorCount?.count || 0;
    summary.fault_events = faultCount?.count || 0;
    summary.wafer_processing_sequences = waferCount?.count || 0;
    summary.rejected_records = rejectCount?.count || 0;

    const recentFiles = await query(
        `SELECT file_id, filename, source_format, parser_version, accepted_count, rejected_count, ingestion_time, inserted_at
         FROM files
         ORDER BY inserted_at DESC
         LIMIT 20`
    );

    const equipment = normalizeRows(
        'equipment_states',
        await query(
            `SELECT id, filename, tool_id, event_ts, curr_state, prev_state, lot, wafer, recipe, step, severity, event_name
             FROM equipment_states
             ORDER BY id DESC
             LIMIT ?`,
            [MAX_ROWS_PER_TABLE]
        )
    );

    const processes = normalizeRows(
        'process_parameters_recipes',
        await query(
            `SELECT id, filename, tool_id, event_ts, recipe, step, parameter, value, unit, status, lot, wafer
             FROM process_parameters_recipes
             ORDER BY id DESC
             LIMIT ?`,
            [MAX_ROWS_PER_TABLE]
        )
    );

    const sensors = normalizeRows(
        'sensor_readings',
        await query(
            `SELECT id, filename, tool_id, event_ts, parameter, value, unit, lot, wafer, recipe, step, severity
             FROM sensor_readings
             ORDER BY id DESC
             LIMIT ?`,
            [MAX_ROWS_PER_TABLE]
        )
    );

    const faults = normalizeRows(
        'fault_events',
        await query(
            `SELECT id, filename, tool_id, event_ts, fault_code, fault_summary, severity, lot, wafer, recipe, step, status
             FROM fault_events
             ORDER BY id DESC
             LIMIT ?`,
            [MAX_ROWS_PER_TABLE]
        )
    );

    const wafer = normalizeRows(
        'wafer_processing_sequences',
        await query(
            `SELECT id, filename, tool_id, event_ts, lot, wafer, slot, recipe, step, status, action, event_name, duration_s, wafer_count
             FROM wafer_processing_sequences
             ORDER BY id DESC
             LIMIT ?`,
            [MAX_ROWS_PER_TABLE]
        )
    );

    return { summary, recentFiles, equipment, processes, sensors, faults, wafer };
}

app.get('/health', async (_req, res) => {
    try {
        await query('SELECT 1 AS ok');
        res.json({ status: 'ok' });
    } catch (error) {
        res.status(500).json({ status: 'error', message: error.message });
    }
});

app.get('/', async (_req, res) => {
    try {
        const data = await loadDashboardData();
        res.render('dashboard', data);
    } catch (error) {
        res.status(500).send(`Database load failed: ${error.message}`);
    }
});

app.post('/analyze-data', async (req, res) => {
    try {
        if (!openai) {
            return res.json({
                analysis: 'OPENAI_API_KEY is not configured. Add it to your .env file to enable AI analysis.'
            });
        }

        const { targetTab, data } = req.body || {};
        const safeData = Array.isArray(data) ? data.slice(0, 50) : [];

        const completion = await openai.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: [
                {
                    role: 'system',
                    content: 'You are a semiconductor manufacturing analyst. Summarize notable patterns, anomalies, and useful next checks based on database rows. Keep the answer concise and practical.'
                },
                {
                    role: 'user',
                    content: `Analyze this dashboard tab: ${targetTab}. Rows: ${JSON.stringify(safeData)}`
                }
            ]
        });

        res.json({ analysis: completion.choices[0]?.message?.content || 'No analysis returned.' });
    } catch (error) {
        res.status(500).json({ error: `AI analysis failed: ${error.message}` });
    }
});

app.listen(PORT, () => {
    console.log(`Dashboard running at http://localhost:${PORT}`);
});
