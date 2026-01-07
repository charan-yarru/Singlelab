import threading
from db.db_handler import DBHandler
from communication.HL7Listener import HL7Listener

def main():
    # Initialize DB connection first
    try:
        db_handler = DBHandler()
        print("✅ DB Connection successful.")
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    # Shared stop signal for listener threads
    stop_event = threading.Event()

    # Function to start the listener based on selected protocol
    def start_listener(port, machine_id, protocol, log_callback):
        stop_event.clear()

        if protocol == "HL7 (TCP/IP)":
            listener = HL7Listener(
                port=int(port),
                machine_id=machine_id,
                log_callback=log_callback,
                stop_event=stop_event,
                db_handler=db_handler
            )
            listener.start()
        else:
            log_callback("❌ Unsupported protocol selected")

    # Function to stop the listener
    def stop_listener():
        stop_event.set()
if __name__ == "__main__":
    main()
