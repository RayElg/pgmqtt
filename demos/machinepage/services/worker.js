const { Client } = require('pg');

const config = {
    host: process.env.PGHOST,
    port: process.env.PGPORT,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE,
};

const machines = ['M-001', 'M-002', 'M-003'];
const state = {};

// Initialize state
machines.forEach(id => {
    state[id] = {
        cpu: 20 + Math.random() * 20,
        mem: 30 + Math.random() * 20,
        temp: 35 + Math.random() * 10,
        status: 'ONLINE'
    };
});

async function simulate() {
    const client = new Client(config);
    try {
        await client.connect();
        console.log('Worker: Connected to Postgres');

        setInterval(async () => {
            const machine_id = machines[Math.floor(Math.random() * machines.length)];
            const s = state[machine_id];

            // Random walk: small deltas (-2 to +2)
            s.cpu = Math.max(0, Math.min(100, s.cpu + (Math.random() * 4 - 2)));
            s.mem = Math.max(0, Math.min(100, s.mem + (Math.random() * 2 - 1)));
            s.temp = Math.max(20, Math.min(90, s.temp + (Math.random() * 1 - 0.5)));

            // Occasionally toggle status
            if (Math.random() > 0.99) {
                s.status = s.status === 'ONLINE' ? 'ERROR' : 'ONLINE';
            }

            try {
                const sql = `UPDATE machine_vitals 
           SET cpu_usage = $1, mem_usage = $2, temperature = $3, status = $4, last_updated = CURRENT_TIMESTAMP
           WHERE machine_id = $5`;
                const params = [s.cpu.toFixed(1), s.mem.toFixed(1), s.temp.toFixed(1), s.status, machine_id];

                console.log(`Worker: Executing Query: ${sql}`);
                console.log(`Worker: With Parameters: [${params.join(', ')}]`);

                await client.query(sql, params);
            } catch (err) {
                console.error('Worker: Update failed', err.message);
            }
        }, 200); // 5Hz throughput

    } catch (err) {
        console.error('Worker: Connection failed', err.stack);
        setTimeout(simulate, 5000);
    }
}

module.exports = { simulate };
