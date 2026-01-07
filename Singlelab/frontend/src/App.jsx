import React, { useEffect, useState } from "react";
import "./App.css";

const resolvedEnvBase =
  import.meta.env.VITE_API_BASE && import.meta.env.VITE_API_BASE.replace(/\/$/, "");

const defaultBase = import.meta.env.DEV
  ? "http://127.0.0.1:8000"
  : `${window.location.protocol}//${window.location.host}`;

const API_BASE = resolvedEnvBase || defaultBase;

const apiFetch = (path, options) =>
  fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

const REFRESH_INTERVAL_MS = 5000;

function App() {
  const [machines, setMachines] = useState([]);
  const [statuses, setStatuses] = useState({});
  const [recentSamples, setRecentSamples] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [actionLoading, setActionLoading] = useState({});

  const fetchMachines = async () => {
    try {
      const res = await apiFetch("/api/machines");
      if (!res.ok) {
        throw new Error(`Machines request failed: ${res.status}`);
      }
      const data = await res.json();
      setMachines(Array.isArray(data) ? data : Object.values(data || {}));
    } catch (fetchError) {
      console.error("Error fetching machines:", fetchError);
      setError("Unable to load machines");
    }
  };

  const fetchStatuses = async () => {
    try {
      const res = await apiFetch("/api/machine-status");
      if (!res.ok) {
        throw new Error(`Status request failed: ${res.status}`);
      }
      const data = await res.json();
      setStatuses(data || {});
    } catch (fetchError) {
      console.error("Error fetching statuses:", fetchError);
      setError("Unable to load statuses");
    }
  };

  const fetchRecentSamples = async (limit = 3) => {
    try {
      const res = await apiFetch(`/api/machine-samples?limit=${limit}`);
      if (!res.ok) {
        throw new Error(`Recent samples request failed: ${res.status}`);
      }
      const data = await res.json();
      setRecentSamples(data || {});
    } catch (fetchError) {
      console.error("Error fetching recent samples:", fetchError);
      setError("Unable to load recent samples");
    }
  };

  const refreshAll = async () => {
    setLoading(true);
    setError("");
    await Promise.all([fetchMachines(), fetchStatuses(), fetchRecentSamples()]);
    setLoading(false);
  };

  const refreshLive = async () => {
    await Promise.all([fetchStatuses(), fetchRecentSamples()]);
  };

  const setMachineActionState = (name, isLoading) => {
    setActionLoading((prev) => ({ ...prev, [name]: isLoading }));
  };

  const sendMachineCommand = async (name, action) => {
    setMachineActionState(name, true);
    setError("");
    try {
      const res = await apiFetch(`/api/machines/${encodeURIComponent(name)}/${action}`, {
        method: "POST",
      });
      let payload = {};
      try {
        payload = await res.json();
      } catch {
        payload = {};
      }
      if (!res.ok) {
        throw new Error(payload.detail || payload.message || `Failed to ${action} ${name}`);
      }
      setStatuses((prev) => ({ ...prev, [name]: payload.state || prev[name] }));
      await Promise.all([fetchStatuses(), fetchRecentSamples()]);
    } catch (actionError) {
      console.error(`Error executing ${action} for ${name}:`, actionError);
      setError(actionError.message);
    } finally {
      setMachineActionState(name, false);
    }
  };

  const onStart = (name) => sendMachineCommand(name, "start");
  const onStop = (name) => sendMachineCommand(name, "stop");
  const onLogs = (name) => {
    const safeName = (name || "machine").replace(/\s+/g, "_");
    const logUrl = `${API_BASE}/logs/${encodeURIComponent(safeName)}.log`;
    window.open(logUrl, "_blank", "noopener,noreferrer");
  };

  useEffect(() => {
    refreshAll();
    const timer = setInterval(refreshLive, REFRESH_INTERVAL_MS);
    return () => clearInterval(timer);
  }, []);

  return (
    <div className="App">
      <header className="app-header">
        <div>
          <p className="app-eyebrow">LIMS Cluster</p>
          <h2>M16LABS</h2>
        </div>
        <button className="ghost" onClick={refreshAll}>
          Refresh machines
        </button>
      </header>
      {loading && <p>Loading...</p>}
      {error && <p className="error">{error}</p>}

      <div className="machine-grid">
        {machines.map((machine) => {
          const status = statuses[machine.name] || "Stopped";
          const isRunning = status === "Running";
          const isBusy = !!actionLoading[machine.name];
          const samples = recentSamples[machine.name] || [];
          const latestSample = samples[0] || null;
          return (
            <div className="machine-card" key={machine.name}>
              <div className="card-header">
                <h3>{machine.name}</h3>
                <span className={`status-pill ${isRunning ? "running" : "stopped"}`}>
                  {isRunning ? "Running" : status}
                </span>
              </div>
              <p className="meta">
                Protocol: <strong>{machine.protocol}</strong>
              </p>
              <p className="meta">Port/Config: {machine.port_display}</p>
              <div className="recent-samples">
                <p className="recent-header">Recent Samples</p>
                <div className="latest-sample">
                  <span>Last updated:</span>
                  <span className="latest-value">
                    {latestSample?.sample_id || "—"}
                  </span>
                  {latestSample?.updated_at && (
                    <span className="latest-time">
                      {new Date(latestSample.updated_at).toLocaleTimeString()}
                    </span>
                  )}
                </div>
                {samples.length === 0 ? (
                  <p className="recent-empty">No recent updates yet.</p>
                ) : (
                  <ul>
                    {samples.map((sample) => (
                      <li key={`${machine.name}-${sample.sample_id}-${sample.updated_at}`}>
                        <span className="sample-id">{sample.sample_id || "N/A"}</span>
                        {sample.updated_at && (
                          <span className="sample-time">
                            {new Date(sample.updated_at).toLocaleTimeString()}
                          </span>
                        )}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
              <div className="actions">
                <button
                  className="primary"
                  onClick={() => onStart(machine.name)}
                  disabled={isRunning || isBusy}
                >
                  {isBusy && !isRunning ? "Starting..." : "Start"}
                </button>
                <button
                  className="danger"
                  onClick={() => onStop(machine.name)}
                  disabled={!isRunning || isBusy}
                >
                  {isBusy && isRunning ? "Stopping..." : "Stop"}
                </button>
                <button className="ghost" onClick={() => onLogs(machine.name)}>
                  Logs
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default App;
