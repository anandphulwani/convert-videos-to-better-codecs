import requests

def call_http_url(message, timeout=10):
    # Open the file in append mode
    with open('call_http_url.txt', 'a') as file:
        file.write(f"{message}\n")
    return
    try:        
        requests.post("https://ntfy.sh/anand_alerts", data=message.encode(encoding='utf-8'), timeout=timeout)
    except requests.exceptions.RequestException as e:
        print(f"Error calling URL: {e}")
        return None
