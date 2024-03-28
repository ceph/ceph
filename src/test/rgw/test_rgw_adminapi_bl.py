import subprocess
import requests
import json
import threading

def get_signature(cmd):
    ps = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return ps.communicate()[0].decode('utf-8').strip()

def test_get_user_info(user_id, should_fail=False):
    try:
        output = subprocess.check_output(['./bin/radosgw-admin', 'user', 'info', '--uid=' + user_id]).decode('utf-8')
        user_info = json.loads(output)
        access_key = user_info['keys'][0]['access_key']
        secret_key = user_info['keys'][0]['secret_key']

        host = "localhost:8000"
        resource = '/admin/user'
        http_query = f'info&uid={user_id}'
        dateTime = subprocess.check_output(['date', '-R', '-u']).decode('utf-8').strip()
        headerToSign = f"GET\n\napplication/x-compressed-tar\n{dateTime}\n{resource}"
        signature_cmd = f'echo -en "{headerToSign}" | openssl sha1 -hmac {secret_key} -binary | base64'
        signature = get_signature(signature_cmd)

        url = f'http://{host}{resource}?{http_query}'
        headers = {
            'Content-Type': 'application/x-compressed-tar', 
            'Date': dateTime,
            'Authorization': f'AWS {access_key}:{signature}',
            'Host': host
        }
        response = requests.get(url, headers=headers)

        if should_fail:
            assert response.status_code != 200, "Expected failure but got success"
        else:
            assert response.status_code == 200, "Failed to get user info"
            print(f"Success: User info retrieved for {user_id}.")
            print(response.json())
    except Exception as e:
        print(f"Error: {str(e)} for user ID {user_id}")

def test_invalid_user_ids():
    invalid_user_ids = ["", "invalid_uid", "bourne"]
    for user_id in invalid_user_ids:
        print(f"Testing with invalid user ID: '{user_id}'")
        test_get_user_info(user_id, should_fail=True)

def test_concurrent_requests():
    user_ids = ["testid", "another_valid_uid", "yet_another_valid_uid"]
    threads = []
    for user_id in user_ids:
        thread = threading.Thread(target=test_get_user_info, args=(user_id,))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    ## valid test id
    print("Testing valid user IDs...")
    test_get_user_info('testid')

    ## test invalid ids
    print("\nTesting invalid user IDs...")
    test_invalid_user_ids()

    # concurrent requests
    print("\nConducting concurrent requests tests...")
    test_concurrent_requests()
