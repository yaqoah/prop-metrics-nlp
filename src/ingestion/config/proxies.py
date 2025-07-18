import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from constants import IP_CHECK_URL, PROXY_TEST_TIMEOUT

def test_proxy(proxy):
    try:
        r = requests.get(IP_CHECK_URL, 
                         proxies={"http": proxy, "https":proxy}, 
                         timeout=PROXY_TEST_TIMEOUT)
        if r.status_code == 200:
            print(f"Valid Proxy: [{proxy}] |||  Success ~")
            return proxy
    except Exception as e:
        print(f"Failed Proxy:[{proxy}]  |||  Error Message: [{e}]")

    return None

def update_proxies():
    with open("proxies.txt", "r") as f:
        proxies = [line.strip() for line in f if line.strip()]

    valid_proxies = []
    with ThreadPoolExecutor(max_workers=100) as executor:
        future_to_proxy = {executor.submit(test_proxy, proxy): proxy for proxy in proxies}
        for future in as_completed(future_to_proxy):
            result = future.result()
            if result:
                valid_proxies.append(result)
                with open("valid_proxies.txt", "a") as f:
                    f.write(result + "\n")
    
    print(f"✅ Found {len(valid_proxies)} / {len(proxies)} valid proxies.")

update_proxies()
