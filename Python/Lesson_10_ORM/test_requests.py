import requests

if __name__ == "__main__":
    r = requests.get("http://localhost:8000/user/all")
    print(r.json())
