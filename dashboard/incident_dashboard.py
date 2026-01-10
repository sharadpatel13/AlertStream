import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import os
from streamlit_autorefresh import st_autorefresh

# ---------------------------
# Config
# ---------------------------
DATA_PATH = "storage/processed/incident_metrics"  # Folder with CSVs
OUTPUT_HTML = "incident_dashboard.html"
SPIKE_THRESHOLD = 3  # Highlight spikes in red
AUTO_REFRESH_INTERVAL = 10000  # 10 seconds

# ---------------------------
# Streamlit page setup & auto-refresh
# ---------------------------
st.set_page_config(page_title="Incident Dashboard", layout="wide")
st_autorefresh(interval=AUTO_REFRESH_INTERVAL, limit=None, key="dashboard_refresh")

st.title("🛠 Incident Monitoring Dashboard")

# ---------------------------
# Read CSV data
# ---------------------------
all_files = [os.path.join(DATA_PATH, f) for f in os.listdir(DATA_PATH) if f.endswith(".csv")]
if not all_files:
    st.warning(f"No CSV files found in {DATA_PATH}")
    st.stop()

df = pd.concat([pd.read_csv(f) for f in all_files], ignore_index=True)

# Ensure columns exist
expected_cols = ["system", "high_severity_count", "window_start", "window_end"]
for col in expected_cols:
    if col not in df.columns:
        df[col] = 0

# Convert timestamps
df["window_start"] = pd.to_datetime(df["window_start"])
df["window_end"] = pd.to_datetime(df["window_end"])

# Sort by time
df = df.sort_values("window_start").reset_index(drop=True)

# ---------------------------
# Summary Metrics
# ---------------------------
summary = df.groupby("system").agg(
    total_high_severity=pd.NamedAgg(column="high_severity_count", aggfunc="sum"),
    avg_per_window=pd.NamedAgg(column="high_severity_count", aggfunc="mean"),
    spikes=pd.NamedAgg(column="high_severity_count", aggfunc=lambda x: (x >= SPIKE_THRESHOLD).sum())
).reset_index()

# Total across all systems
total_incidents = df["high_severity_count"].sum()
total_spikes = (df["high_severity_count"] >= SPIKE_THRESHOLD).sum()
avg_incidents_overall = df["high_severity_count"].mean()

# ---------------------------
# Summary Cards
# ---------------------------
col1, col2, col3 = st.columns(3)
col1.metric("Total Incidents", total_incidents)
col2.metric("Average Incidents per Window", f"{avg_incidents_overall:.2f}")
col3.metric("Spikes (>=3)", total_spikes)

# ---------------------------
# Select system
# ---------------------------
systems = df["system"].unique().tolist()
selected_system = st.selectbox("Select System", ["All"] + systems)

if selected_system != "All":
    filtered_df = df[df["system"] == selected_system].copy()
else:
    filtered_df = df.copy()

# ---------------------------
# Time-Series Plot with Spikes
# ---------------------------
fig = go.Figure()

# Normal points
normal_points = filtered_df[filtered_df["high_severity_count"] < SPIKE_THRESHOLD]
fig.add_trace(go.Scatter(
    x=normal_points["window_start"],
    y=normal_points["high_severity_count"],
    mode="lines+markers",
    name="Incidents",
    line=dict(color="blue"),
    marker=dict(size=8),
    hovertemplate=(
        "System: %{customdata[0]}<br>" +
        "Window Start: %{customdata[1]|%Y-%m-%d %H:%M}<br>" +
        "Window End: %{customdata[2]|%Y-%m-%d %H:%M}<br>" +
        "Count: %{y}<extra></extra>"
    ),
    customdata=normal_points[["system", "window_start", "window_end"]].values
))

# Spike points
spike_points = filtered_df[filtered_df["high_severity_count"] >= SPIKE_THRESHOLD]
fig.add_trace(go.Scatter(
    x=spike_points["window_start"],
    y=spike_points["high_severity_count"],
    mode="markers",
    name="Spike",
    marker=dict(color="red", size=12, symbol="circle-open"),
    hovertemplate=(
        "System: %{customdata[0]}<br>" +
        "Window Start: %{customdata[1]|%Y-%m-%d %H:%M}<br>" +
        "Window End: %{customdata[2]|%Y-%m-%d %H:%M}<br>" +
        "Count: %{y}<extra></extra>"
    ),
    customdata=spike_points[["system", "window_start", "window_end"]].values
))

fig.update_layout(
    title=f"High Severity Incidents Over Time ({selected_system})",
    xaxis_title="Window Start",
    yaxis_title="High Severity Count",
    template="plotly_white",
    hovermode="x unified"
)

st.plotly_chart(fig, use_container_width=True)

# ---------------------------
# Show Summary Table
# ---------------------------
st.subheader("System Metrics Summary")
st.dataframe(summary)

# ---------------------------
# Save HTML dashboard
# ---------------------------
fig.write_html(OUTPUT_HTML)
st.markdown(f"**Interactive dashboard saved as HTML:** [{OUTPUT_HTML}]({OUTPUT_HTML})")
