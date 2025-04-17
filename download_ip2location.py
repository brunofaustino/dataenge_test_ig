from ipwhois import IPWhois
import socket

ip = socket.gethostbyname('nike.com.br')
whois = IPWhois(ip)
result = whois.lookup_rdap()
print(result['network']['country'])
