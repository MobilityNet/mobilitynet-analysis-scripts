import requests
import arrow
import copy

def register_label(server_url, phone_label):
    """
    Register the phone label on the specified URL. Used to create the phone
    account on the dest server so that we can upload data to it
    """
    post_msg = {
        "user": phone_label,
    }
    print("About to retrieve messages using %s" % post_msg)
    response = requests.post(server_url+"/profile/create", json=post_msg)
    print("response = %s" % response)
    response.raise_for_status()
    local_uuid = response.json()["uuid"]
    print("Found local uuid %s" % local_uuid)
    return local_uuid

def post_entries(server_url, phone_label, entry_list):
    """
    Post the entries for the specified user. The uuid in the entry_list could
    be different from the uuid on the server, since it has been retrieved from
    a different server. So we rewrite the uuid before posting.
    """

    stripped_entries = [_strip_id_user(e) for e in entry_list]

    post_msg = {
        "user": phone_label,
        "phone_to_server": stripped_entries
    }
    print("About to post %d messages (%s)..." % (len(stripped_entries), stripped_entries[0]))
    response = requests.post(server_url+"/usercache/put", json=post_msg)
    print("response = %s" % response)
    response.raise_for_status()

def _strip_id_user(e):
    ce = copy.copy(e)
    del ce["_id"]
    del ce["user_id"]
    return ce
