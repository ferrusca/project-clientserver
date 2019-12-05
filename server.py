#!/usr/bin/python3

import sys
import socket
import re 
import traceback

HOST = '127.0.0.1'
PORT = int(sys.argv[1])
ENCODING = 'utf-8'

semicolon_pattern = r';$'
insert_pattern = r'INSERT INTO (\S+)[ ]{0,1}\((\S+)\) VALUES \((\S+)\)( , \((\S+)\))*;'
update_pattern = r'update (\S+) set (\S+)=(\S+)(,[ ]{0,1}((\S+)=(\S+))+)* where (\S+)=(\S+);'
select_pattern = r'select (\S+) from (\S+)( where ((\S+)=(\S+))){0,1};'
delete_pattern = r'delete from (\S+)( where (\S+)=(\S+)){0,1};'



def createSocket(port, host='0.0.0.0'):
    '''
        Función para la creación de un socket
        :param port: puerto donde que se ocupará para abrir el socket
        :param host: host para permitir acceso al socket, por defecto está abierto a todos
        :return: retorna el socket
    '''
    mySocket = socket.socket()
    mySocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    mySocket.bind((host,port))
    mySocket.listen(1)
    return mySocket

sock = createSocket(PORT)
print('Server listening in', PORT)

def send_message(connection, message):
    connection.sendall(bytes(message, ENCODING))

def retrieve_attributes(table_name):
    try: 
        with open(table_name.upper(), 'r') as fileHandle:
            firstline = fileHandle.readline().strip()
            lineList = fileHandle.readlines()
            last_id = 1 if len(lineList) < 2 else int(lineList[len(lineList)-1].split(',')[0])
            return firstline.split(',')
    except:
        print("Error al obtener la tabla")

def retrieve_last_id(table_name):
    try: 
        with open(table_name.upper(), 'r') as fileHandle:
            firstline = fileHandle.readline().strip()
            lineList = fileHandle.readlines()
            return 1 if len(lineList) < 2 else int(lineList[len(lineList)-1].split(',')[0])
    except:
        print("Error al obtener la tabla")    

def insert(match_obj):
    table_name = match_obj.group(1).strip().upper()
    user_attributes = match_obj.group(2).strip().upper().split(',')
    user_values = match_obj.group(3).strip().split(',')
    if (len(user_attributes) != len(user_values)):
        print('Falta un campo en ' + 'atributos' if len(user_attributes) < len(user_attributes) else 'el campo VALUES')
        return
    with open(table_name, 'a') as output:
        table_attributes = retrieve_attributes(table_name)
        last_id = retrieve_last_id(table_name)
        output.write('\n')
        output.write(str(last_id+1))
        for attribute in table_attributes:
            print(attribute)
            try:
                index = user_attributes.index(attribute)
                output.write(user_values[index])
            except:
                print("No tenia valor", attribute)
            finally:
                output.write(',')
    return 'Registro insertado con éxito'

def select(match_obj):
    select_attributes = match_obj.group(1).strip().upper().split(',')
    table_name = match_obj.group(2).strip().upper()
    print(match_obj.string)
    if 'where' in match_obj.string:
        attribute_selected, value_selected = match_obj.group(4).strip().upper().split('=')
        index_to_select = retrieve_attributes(table_name).index(attribute_selected)
        select_condition = True
    else:
        select_condition = False
    rows_to_return = []
    try:
        with open(table_name, 'r') as fileHandler:
            for line in fileHandler:
                print(line)
                if (select_condition):
                    if line.split(',')[index_to_select] == value_selected:
                        rows_to_return.append(line)
                else:    
                    print("apend all")
                    rows_to_return.append(line)
        return_message = ''
        for row in rows_to_return:
            return_message += str(row)
            return_message += '\n'
        print(return_message)
        return return_message
    except:
        print("Error al obtener la tabla")

def parse_command(command):
    semicolon = re.search(semicolon_pattern, command, re.IGNORECASE)
    if not semicolon: 
        return "Falta punto y coma"
    match_obj = re.search(insert_pattern, command, re.IGNORECASE)
    if match_obj:
        return insert(match_obj)
    print("not match insert")
    match_obj = re.search(select_pattern, command, re.IGNORECASE)
    if match_obj:
        return select(match_obj)
    print("not match select")
    match_obj = re.search(update_pattern, command, re.IGNORECASE)
    if match_obj:
        return update(match_obj)
    print("not match update")
    match_obj = re.search(delete_pattern, command, re.IGNORECASE)
    if match_obj:
        return delete(match_obj)
    return "Comando no válido. verifique"

while True:
    # Obtenemos la dirección y la conexión del socket
    conn, addr = sock.accept()
    print ("Connection from: " + str(addr))
    send_message(conn, "Ingresa un comando:")
    # Obtenemos los datos enviados por el socket
    while True:
        try:
            data = conn.recv(1024).decode().strip()
            if (data == 'exit'):
                print("User has exited")
                send_message(conn, 'FIN')
                conn.close()
                break;
            if data and data != '':
                print("El comando del cliente es:",data)
                send_message(conn, parse_command(data))
        except Exception:
            traceback.print_exc()
            conn.close()
    # user has entered 'exit'
