import ssl
import paho.mqtt.client as mqtt

client_id = "GID_User@@@yk_2507140ERW880TN8GE8"
username = "Signature|LTAI5tSM5NMEPtxSWti5JF8P|post-cn-o843ro4yk01"
password = "TXS/IhZfhWQuB6cPE6TXoD4Equ8="
broker = "post-cn-o843ro4yk01.mqtt.aliyuncs.com"
port = 443
topics = [
    "pigeon/auctions/245/#",
    "auction/auctions/245",
    "bid/pigeons/180808",
    "currentpigeon/auctions/245"
]

def on_connect(client, userdata, flags, rc):
    print(f"连接结果: {rc}")
    if rc == 0:
        for t in topics:
            client.subscribe(t)
            print(f"订阅主题: {t}")
    else:
        print("连接失败")

def on_subscribe(client, userdata, mid, granted_qos):
    print(f"订阅确认 mid={mid} qos={granted_qos}")

def on_message(client, userdata, msg):
    print(f"收到消息 Topic={msg.topic}")
    print(f"Payload={msg.payload.decode('utf-8', errors='ignore')}")

def on_disconnect(client, userdata, rc):
    print(f"断开连接 rc={rc}")
    if rc != 0:
        print("意外断开，尝试重连")

client = mqtt.Client(client_id=client_id, transport="websockets")
client.username_pw_set(username, password)
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_disconnect = on_disconnect

client.connect(broker, port)
client.loop_forever()
