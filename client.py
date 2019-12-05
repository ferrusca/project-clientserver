#!/usr/bin/python
# -*- coding: utf-8 -*-
# FERRUSCA 
# RÍOS MORALES ALEXIS

import socket
import sys

HOST = '127.0.0.1'
PORT = int(sys.argv[1])
PROMPT = 'ORACLE-DB> '

# command = 'insert into alumno (nombre,apellido,carrera) values (jorge,ferrusca,compu);'
# command = 'select * from alumno where id=3;'
# command = 'delete from alumno where id=3;'
# command = 'delete from alumno where id=3;'
# command = 'update alumno set Apellido=Ríossss WHERE id=2;'


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    data = ''
    print(data)
    while data != 'FIN':
        data = s.recv(1024).decode()
        if (data == 'FIN'):
            break
        if (data != ''):
            print(data)
        command = input(PROMPT)
        s.sendall(bytes(command.strip(), 'utf-8'))
    s.close()
print("Vuelva Pronto :)")
