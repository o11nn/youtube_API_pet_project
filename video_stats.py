import requests
API_KEY = "AIzaSyArVLLDC2q7bhBEBUWwpGVLmaIxPBgVqqU"
CHANNEL_HANDLE = "MrBeast"
url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

response = requests.get(url)
print(response)