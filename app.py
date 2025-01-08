from flask import Flask, render_template
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from apscheduler.schedulers.background import BackgroundScheduler
import threading

app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler_lock = threading.Lock()

# Global data storage for UI
insights = {
    "total_transactions": 0,
    "category_breakdown": [],
    "geographic_distribution": [],
    "status_analysis": []
}

def fetch_insights():
    global insights
    try:
        # Connect to Cassandra
        auth_provider = PlainTextAuthProvider('cassandra', 'cassandra')
        cluster = Cluster(['172.23.0.6'], auth_provider=auth_provider)
        session = cluster.connect('transactionkeyspace')

        # Query data
        total_transactions = session.execute("SELECT total FROM total_transactions").one().total
        category_breakdown = session.execute("SELECT category, count FROM category_breakdown")
        geographic_distribution = session.execute("SELECT location, count FROM geographic_distribution")
        status_analysis = session.execute("SELECT status, count FROM status_analysis")

        # Update insights
        with scheduler_lock:
            insights["total_transactions"] = total_transactions
            insights["category_breakdown"] = [{"category": row.category, "count": row.count} for row in category_breakdown]
            insights["geographic_distribution"] = [{"location": row.location, "count": row.count} for row in geographic_distribution]
            insights["status_analysis"] = [{"status": row.status, "count": row.count} for row in status_analysis]

        cluster.shutdown()

    except Exception as e:
        print(f"Error fetching insights: {e}")

# Schedule data fetching every minute
scheduler.add_job(fetch_insights, 'interval', minutes=1)
scheduler.start()

@app.route("/")
def dashboard():
    with scheduler_lock:
        return render_template("dashboard.html", insights=insights)

if __name__ == "__main__":
    fetch_insights()  # Fetch data initially
    app.run(host="0.0.0.0", port=5000)
