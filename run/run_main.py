import socket

# 获取主机名
hostname = socket.gethostname()

# 获取本机 IP 地址
ip_address = socket.gethostbyname(hostname)

print("主机名:", hostname)
print("本机 IP 地址:", ip_address)