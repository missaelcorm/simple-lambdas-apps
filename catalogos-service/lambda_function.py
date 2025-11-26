import json
import boto3
import uuid
import re
import os
from datetime import datetime
from decimal import Decimal

# Variables de entorno
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'local')
TABLE_CLIENTES = os.environ.get('TABLE_CLIENTES', 'Clientes')
TABLE_DOMICILIOS = os.environ.get('TABLE_DOMICILIOS', 'Domicilios')
TABLE_PRODUCTOS = os.environ.get('TABLE_PRODUCTOS', 'Productos')

# Clientes AWS
dynamodb = boto3.resource('dynamodb')

# Regex para validación
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
RFC_REGEX = re.compile(r'^[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{2,3}$')

# Helper para convertir Decimal a tipos JSON serializables
def decimal_to_json(obj):
    if isinstance(obj, Decimal):
        return float(obj) if obj % 1 else int(obj)
    raise TypeError

def response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(body, default=decimal_to_json)
    }

def validate_cliente(data):
    required = ['razon_social', 'nombre_comercial', 'rfc', 'correo', 'telefono']
    for field in required:
        if field not in data or not data[field]:
            return False, f"Campo requerido: {field}"

    # Validar formato RFC
    if not RFC_REGEX.match(data['rfc'].upper()):
        return False, "RFC no tiene un formato válido"

    # Validar formato email
    if not EMAIL_REGEX.match(data['correo']):
        return False, "Correo electrónico no tiene un formato válido"

    return True, None

def validate_domicilio(data):
    required = ['cliente_id', 'domicilio', 'colonia', 'municipio', 'estado', 'tipo']
    for field in required:
        if field not in data or not data[field]:
            return False, f"Campo requerido: {field}"

    if data['tipo'].upper() not in ['FACTURACION', 'ENVIO']:
        return False, "Tipo debe ser FACTURACION o ENVIO"

    return True, None

def validate_producto(data):
    required = ['nombre', 'unidad_medida', 'precio_base']
    for field in required:
        if field not in data or not data[field]:
            return False, f"Campo requerido: {field}"

    try:
        precio = Decimal(str(data['precio_base']))
        if precio <= 0:
            return False, "Precio base debe ser mayor a 0"
    except:
        return False, "Precio base debe ser un número válido"

    return True, None

# ============= CRUD CLIENTES =============
def create_cliente(data):
    valid, error = validate_cliente(data)
    if not valid:
        return response(400, {'error': error})

    table = dynamodb.Table(TABLE_CLIENTES)
    cliente_id = str(uuid.uuid4())

    item = {
        'id': cliente_id,
        'razon_social': data['razon_social'],
        'nombre_comercial': data['nombre_comercial'],
        'rfc': data['rfc'].upper(),
        'correo': data['correo'].lower(),
        'telefono': data['telefono'],
        'created_at': datetime.now().isoformat()
    }

    table.put_item(Item=item)
    return response(201, item)

def get_cliente(cliente_id):
    table = dynamodb.Table(TABLE_CLIENTES)
    result = table.get_item(Key={'id': cliente_id})

    if 'Item' not in result:
        return response(404, {'error': 'Cliente no encontrado'})

    return response(200, result['Item'])

def list_clientes():
    table = dynamodb.Table(TABLE_CLIENTES)
    result = table.scan()
    return response(200, result.get('Items', []))

def update_cliente(cliente_id, data):
    valid, error = validate_cliente(data)
    if not valid:
        return response(400, {'error': error})

    table = dynamodb.Table(TABLE_CLIENTES)

    # Verificar que existe
    existing = table.get_item(Key={'id': cliente_id})
    if 'Item' not in existing:
        return response(404, {'error': 'Cliente no encontrado'})

    item = {
        'id': cliente_id,
        'razon_social': data['razon_social'],
        'nombre_comercial': data['nombre_comercial'],
        'rfc': data['rfc'].upper(),
        'correo': data['correo'].lower(),
        'telefono': data['telefono'],
        'updated_at': datetime.now().isoformat()
    }

    table.put_item(Item=item)
    return response(200, item)

def delete_cliente(cliente_id):
    table = dynamodb.Table(TABLE_CLIENTES)

    # Verificar que existe
    existing = table.get_item(Key={'id': cliente_id})
    if 'Item' not in existing:
        return response(404, {'error': 'Cliente no encontrado'})

    table.delete_item(Key={'id': cliente_id})
    return response(200, {'message': 'Cliente eliminado correctamente'})

# ============= CRUD DOMICILIOS =============
def create_domicilio(data):
    valid, error = validate_domicilio(data)
    if not valid:
        return response(400, {'error': error})

    # Verificar que el cliente existe
    cliente_table = dynamodb.Table(TABLE_CLIENTES)
    cliente = cliente_table.get_item(Key={'id': data['cliente_id']})
    if 'Item' not in cliente:
        return response(404, {'error': 'Cliente no encontrado'})

    table = dynamodb.Table(TABLE_DOMICILIOS)
    domicilio_id = str(uuid.uuid4())

    item = {
        'id': domicilio_id,
        'cliente_id': data['cliente_id'],
        'domicilio': data['domicilio'],
        'colonia': data['colonia'],
        'municipio': data['municipio'],
        'estado': data['estado'],
        'tipo': data['tipo'].upper(),
        'created_at': datetime.now().isoformat()
    }

    table.put_item(Item=item)
    return response(201, item)

def get_domicilio(domicilio_id):
    table = dynamodb.Table(TABLE_DOMICILIOS)
    result = table.get_item(Key={'id': domicilio_id})

    if 'Item' not in result:
        return response(404, {'error': 'Domicilio no encontrado'})

    return response(200, result['Item'])

def list_domicilios():
    table = dynamodb.Table(TABLE_DOMICILIOS)
    result = table.scan()
    return response(200, result.get('Items', []))

def update_domicilio(domicilio_id, data):
    valid, error = validate_domicilio(data)
    if not valid:
        return response(400, {'error': error})

    table = dynamodb.Table(TABLE_DOMICILIOS)

    # Verificar que existe
    existing = table.get_item(Key={'id': domicilio_id})
    if 'Item' not in existing:
        return response(404, {'error': 'Domicilio no encontrado'})

    item = {
        'id': domicilio_id,
        'cliente_id': data['cliente_id'],
        'domicilio': data['domicilio'],
        'colonia': data['colonia'],
        'municipio': data['municipio'],
        'estado': data['estado'],
        'tipo': data['tipo'].upper(),
        'updated_at': datetime.now().isoformat()
    }

    table.put_item(Item=item)
    return response(200, item)

def delete_domicilio(domicilio_id):
    table = dynamodb.Table(TABLE_DOMICILIOS)

    # Verificar que existe
    existing = table.get_item(Key={'id': domicilio_id})
    if 'Item' not in existing:
        return response(404, {'error': 'Domicilio no encontrado'})

    table.delete_item(Key={'id': domicilio_id})
    return response(200, {'message': 'Domicilio eliminado correctamente'})

# ============= CRUD PRODUCTOS =============
def create_producto(data):
    valid, error = validate_producto(data)
    if not valid:
        return response(400, {'error': error})

    table = dynamodb.Table(TABLE_PRODUCTOS)
    producto_id = str(uuid.uuid4())

    item = {
        'id': producto_id,
        'nombre': data['nombre'],
        'unidad_medida': data['unidad_medida'],
        'precio_base': Decimal(str(data['precio_base'])),
        'created_at': datetime.now().isoformat()
    }

    table.put_item(Item=item)
    return response(201, item)

def get_producto(producto_id):
    table = dynamodb.Table(TABLE_PRODUCTOS)
    result = table.get_item(Key={'id': producto_id})

    if 'Item' not in result:
        return response(404, {'error': 'Producto no encontrado'})

    return response(200, result['Item'])

def list_productos():
    table = dynamodb.Table(TABLE_PRODUCTOS)
    result = table.scan()
    return response(200, result.get('Items', []))

def update_producto(producto_id, data):
    valid, error = validate_producto(data)
    if not valid:
        return response(400, {'error': error})

    table = dynamodb.Table(TABLE_PRODUCTOS)

    # Verificar que existe
    existing = table.get_item(Key={'id': producto_id})
    if 'Item' not in existing:
        return response(404, {'error': 'Producto no encontrado'})

    item = {
        'id': producto_id,
        'nombre': data['nombre'],
        'unidad_medida': data['unidad_medida'],
        'precio_base': Decimal(str(data['precio_base'])),
        'updated_at': datetime.now().isoformat()
    }

    table.put_item(Item=item)
    return response(200, item)

def delete_producto(producto_id):
    table = dynamodb.Table(TABLE_PRODUCTOS)

    # Verificar que existe
    existing = table.get_item(Key={'id': producto_id})
    if 'Item' not in existing:
        return response(404, {'error': 'Producto no encontrado'})

    table.delete_item(Key={'id': producto_id})
    return response(200, {'message': 'Producto eliminado correctamente'})

# ============= HANDLER PRINCIPAL =============
def lambda_handler(event, context):
    print(f"Event: {json.dumps(event)}")
    print(f"Environment: {ENVIRONMENT}")

    http_method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method'))
    path = event.get('path', event.get('rawPath', ''))

    # Parse body
    body = {}
    if event.get('body'):
        try:
            body = json.loads(event['body'])
        except:
            pass

    # Path parameters
    path_params = event.get('pathParameters') or {}
    resource_id = path_params.get('id')

    # Routing
    try:
        # CLIENTES
        if '/clientes' in path:
            if http_method == 'POST' and resource_id is None:
                return create_cliente(body)
            elif http_method == 'GET' and resource_id:
                return get_cliente(resource_id)
            elif http_method == 'GET':
                return list_clientes()
            elif http_method == 'PUT' and resource_id:
                return update_cliente(resource_id, body)
            elif http_method == 'DELETE' and resource_id:
                return delete_cliente(resource_id)

        # DOMICILIOS
        elif '/domicilios' in path:
            if http_method == 'POST' and resource_id is None:
                return create_domicilio(body)
            elif http_method == 'GET' and resource_id:
                return get_domicilio(resource_id)
            elif http_method == 'GET':
                return list_domicilios()
            elif http_method == 'PUT' and resource_id:
                return update_domicilio(resource_id, body)
            elif http_method == 'DELETE' and resource_id:
                return delete_domicilio(resource_id)

        # PRODUCTOS
        elif '/productos' in path:
            if http_method == 'POST' and resource_id is None:
                return create_producto(body)
            elif http_method == 'GET' and resource_id:
                return get_producto(resource_id)
            elif http_method == 'GET':
                return list_productos()
            elif http_method == 'PUT' and resource_id:
                return update_producto(resource_id, body)
            elif http_method == 'DELETE' and resource_id:
                return delete_producto(resource_id)

        return response(404, {'error': 'Endpoint no encontrado'})

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return response(500, {'error': f'Error interno: {str(e)}'})
