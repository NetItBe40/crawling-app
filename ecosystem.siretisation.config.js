module.exports = {
  apps: [0,1,2,3].map(i => ({
    name: `siretisation-w${i}`,
    script: "modules/siretisation.py",
    interpreter: "/home/netit972/crawling-app/venv/bin/python3",
    cwd: "/home/netit972/crawling-app",
    env: { NUM_WORKERS: "4", WORKER_ID: String(i), PYTHONPATH: "/home/netit972/crawling-app" }
  }))
};
