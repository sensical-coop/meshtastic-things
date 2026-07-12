# TODO make this inspect payload?
def get_key(topic, payload):
    parts = topic.value.split('/')
    return parts[-1].encode()