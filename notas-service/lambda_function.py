import json
import boto3
import uuid
import os
from datetime import datetime
from decimal import Decimal
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer

# Variables de entorno
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'local')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'notas-venta-bucket')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')
TABLE_CLIENTES = os.environ.get('TABLE_CLIENTES', 'Clientes')
TABLE_DOMICILIOS = os.environ.get('TABLE_DOMICILIOS', 'Domicilios')
TABLE_PRODUCTOS = os.environ.get('TABLE_PRODUCTOS', 'Productos')
TABLE_NOTAS = os.environ.get('TABLE_NOTAS', 'NotasVenta')
TABLE_CONTENIDO_NOTAS = os.environ.get('TABLE_CONTENIDO_NOTAS', 'ContenidoNotasVenta')

# Clientes AWS
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
sns = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')

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

def validate_nota_venta(data):
    required = ['cliente_id', 'direccion_facturacion_id', 'direccion_envio_id', 'productos']
    for field in required:
        if field not in data or not data[field]:
            return False, f"Campo requerido: {field}"

    if not isinstance(data['productos'], list) or len(data['productos']) == 0:
        return False, "Debe incluir al menos un producto"

    for producto in data['productos']:
        if 'producto_id' not in producto or 'cantidad' not in producto:
            return False, "Cada producto debe tener producto_id y cantidad"
        try:
            if int(producto['cantidad']) <= 0:
                return False, "Cantidad debe ser mayor a 0"
        except:
            return False, "Cantidad debe ser un número válido"

    return True, None

# ============= GENERACIÓN DE PDF =============
def generar_pdf(nota, cliente, contenidos):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []
    styles = getSampleStyleSheet()

    # Título
    title = Paragraph("<b>NOTA DE VENTA</b>", styles['Title'])
    elements.append(title)
    elements.append(Spacer(1, 12))

    # Información del cliente
    elements.append(Paragraph(f"<b>Razón Social:</b> {cliente['razon_social']}", styles['Normal']))
    elements.append(Paragraph(f"<b>Nombre Comercial:</b> {cliente['nombre_comercial']}", styles['Normal']))
    elements.append(Paragraph(f"<b>RFC:</b> {cliente['rfc']}", styles['Normal']))
    elements.append(Paragraph(f"<b>Correo:</b> {cliente['correo']}", styles['Normal']))
    elements.append(Paragraph(f"<b>Teléfono:</b> {cliente['telefono']}", styles['Normal']))
    elements.append(Spacer(1, 12))

    # Información de la nota
    elements.append(Paragraph(f"<b>Folio:</b> {nota['folio']}", styles['Normal']))
    elements.append(Spacer(1, 12))

    # Tabla de productos
    table_data = [['Cantidad', 'Producto', 'Precio Unitario', 'Importe']]
    for item in contenidos:
        table_data.append([
            str(item['cantidad']),
            item['producto_nombre'],
            f"${float(item['precio_unitario']):.2f}",
            f"${float(item['importe']):.2f}"
        ])

    # Total
    table_data.append(['', '', 'TOTAL:', f"${float(nota['total']):.2f}"])

    table = Table(table_data, colWidths=[80, 200, 100, 100])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, -1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))

    elements.append(table)
    doc.build(elements)

    buffer.seek(0)
    return buffer

# ============= SUBIDA A S3 CON METADATOS =============
def subir_pdf_s3(pdf_buffer, rfc_cliente, folio):
    key = f"{rfc_cliente}/{folio}.pdf"

    # Obtener metadatos actuales si existen
    veces_enviado = 1
    try:
        obj_metadata = s3.head_object(Bucket=BUCKET_NAME, Key=key)
        if 'Metadata' in obj_metadata and 'veces-enviado' in obj_metadata['Metadata']:
            veces_enviado = int(obj_metadata['Metadata']['veces-enviado']) + 1
    except:
        pass

    metadata = {
        'hora-envio': datetime.now().isoformat(),
        'nota-descargada': 'false',
        'veces-enviado': str(veces_enviado)
    }

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=pdf_buffer.getvalue(),
        ContentType='application/pdf',
        Metadata=metadata
    )

    return key

# ============= PUBLICAR MENSAJE PARA NOTIFICACIÓN =============
def publicar_evento_notificacion(cliente_email, folio, rfc_cliente, api_gateway_url):
    """Publica un mensaje SNS para que notifications-service envíe el email"""

    message_data = {
        'email': cliente_email,
        'folio': folio,
        'rfc': rfc_cliente,
        'api_gateway_url': api_gateway_url,
        'timestamp': datetime.now().isoformat()
    }

    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f'Nueva Nota de Venta - {folio}',
            Message=json.dumps(message_data),
            MessageAttributes={
                'folio': {'DataType': 'String', 'StringValue': folio},
                'rfc': {'DataType': 'String', 'StringValue': rfc_cliente},
                'email': {'DataType': 'String', 'StringValue': cliente_email}
            }
        )
        print(f"Evento de notificación publicado para folio {folio}")
        return True
    except Exception as e:
        print(f"Error publicando evento de notificación: {str(e)}")
        return False

# ============= NOTAS DE VENTA =============
def create_nota_venta(data, api_gateway_url):
    # Timestamp de inicio para métrica de tiempo de ejecución
    start_time = datetime.now()

    valid, error = validate_nota_venta(data)
    if not valid:
        return response(400, {'error': error})

    # Verificar que el cliente existe
    cliente_table = dynamodb.Table(TABLE_CLIENTES)
    cliente_result = cliente_table.get_item(Key={'id': data['cliente_id']})
    if 'Item' not in cliente_result:
        return response(404, {'error': 'Cliente no encontrado'})
    cliente = cliente_result['Item']

    # Verificar direcciones
    domicilio_table = dynamodb.Table(TABLE_DOMICILIOS)
    dir_fact = domicilio_table.get_item(Key={'id': data['direccion_facturacion_id']})
    dir_envio = domicilio_table.get_item(Key={'id': data['direccion_envio_id']})

    if 'Item' not in dir_fact:
        return response(404, {'error': 'Dirección de facturación no encontrada'})
    if 'Item' not in dir_envio:
        return response(404, {'error': 'Dirección de envío no encontrada'})

    # Calcular total y crear contenidos
    producto_table = dynamodb.Table(TABLE_PRODUCTOS)
    total = Decimal('0')
    contenidos = []

    for item in data['productos']:
        producto_result = producto_table.get_item(Key={'id': item['producto_id']})
        if 'Item' not in producto_result:
            return response(404, {'error': f"Producto {item['producto_id']} no encontrado"})

        producto = producto_result['Item']
        cantidad = Decimal(str(item['cantidad']))
        precio_unitario = producto['precio_base']
        importe = cantidad * precio_unitario
        total += importe

        contenidos.append({
            'producto_id': item['producto_id'],
            'producto_nombre': producto['nombre'],
            'cantidad': cantidad,
            'precio_unitario': precio_unitario,
            'importe': importe
        })

    # Crear nota de venta
    nota_id = str(uuid.uuid4())
    folio = f"NV-{datetime.now().strftime('%Y%m%d')}-{nota_id[:8].upper()}"

    nota = {
        'id': nota_id,
        'folio': folio,
        'cliente_id': data['cliente_id'],
        'direccion_facturacion_id': data['direccion_facturacion_id'],
        'direccion_envio_id': data['direccion_envio_id'],
        'total': total,
        'created_at': datetime.now().isoformat()
    }

    # Guardar en DynamoDB
    notas_table = dynamodb.Table(TABLE_NOTAS)
    notas_table.put_item(Item=nota)

    # Guardar contenido
    contenido_table = dynamodb.Table(TABLE_CONTENIDO_NOTAS)
    for contenido in contenidos:
        contenido_id = str(uuid.uuid4())
        contenido_item = {
            'id': contenido_id,
            'nota_id': nota_id,
            'producto_id': contenido['producto_id'],
            'cantidad': contenido['cantidad'],
            'precio_unitario': contenido['precio_unitario'],
            'importe': contenido['importe']
        }
        contenido_table.put_item(Item=contenido_item)

    # Generar PDF
    pdf_buffer = generar_pdf(nota, cliente, contenidos)

    # Subir a S3
    s3_key = subir_pdf_s3(pdf_buffer, cliente['rfc'], folio)

    # Publicar evento para notificación
    publicar_evento_notificacion(cliente['correo'], folio, cliente['rfc'], api_gateway_url)

    # Calcular tiempo de ejecución
    end_time = datetime.now()
    execution_time_ms = (end_time - start_time).total_seconds() * 1000

    # Enviar métricas a CloudWatch
    try:
        cloudwatch.put_metric_data(
            Namespace='NotasVentaApp',
            MetricData=[
                {
                    'MetricName': 'NotaVentaCreada',
                    'Dimensions': [
                        {'Name': 'Environment', 'Value': ENVIRONMENT},
                        {'Name': 'Service', 'Value': 'notas-service'}
                    ],
                    'Value': 1,
                    'Unit': 'Count',
                    'Timestamp': datetime.now()
                },
                {
                    'MetricName': 'TiempoEjecucionNotaVenta',
                    'Dimensions': [
                        {'Name': 'Environment', 'Value': ENVIRONMENT},
                        {'Name': 'Service', 'Value': 'notas-service'}
                    ],
                    'Value': execution_time_ms,
                    'Unit': 'Milliseconds',
                    'Timestamp': datetime.now()
                }
            ]
        )
    except Exception as e:
        print(f"Error enviando métricas: {str(e)}")

    return response(201, {
        'nota': nota,
        'contenidos': contenidos,
        's3_key': s3_key,
        'execution_time_ms': execution_time_ms,
        'message': 'Nota de venta creada y evento de notificación publicado'
    })

def get_nota_venta(nota_id):
    # Obtener nota
    notas_table = dynamodb.Table(TABLE_NOTAS)
    nota_result = notas_table.get_item(Key={'id': nota_id})

    if 'Item' not in nota_result:
        return response(404, {'error': 'Nota de venta no encontrada'})

    nota = nota_result['Item']

    # Obtener cliente
    cliente_table = dynamodb.Table(TABLE_CLIENTES)
    cliente = cliente_table.get_item(Key={'id': nota['cliente_id']})['Item']

    # Obtener direcciones
    domicilio_table = dynamodb.Table(TABLE_DOMICILIOS)
    dir_fact = domicilio_table.get_item(Key={'id': nota['direccion_facturacion_id']})['Item']
    dir_envio = domicilio_table.get_item(Key={'id': nota['direccion_envio_id']})['Item']

    # Obtener contenidos
    contenido_table = dynamodb.Table(TABLE_CONTENIDO_NOTAS)
    contenidos_result = contenido_table.scan(
        FilterExpression='nota_id = :nota_id',
        ExpressionAttributeValues={':nota_id': nota_id}
    )

    contenidos = []
    for item in contenidos_result.get('Items', []):
        # Obtener información del producto
        producto_table = dynamodb.Table(TABLE_PRODUCTOS)
        producto = producto_table.get_item(Key={'id': item['producto_id']})['Item']

        contenidos.append({
            'id': item['id'],
            'producto': producto,
            'cantidad': item['cantidad'],
            'precio_unitario': item['precio_unitario'],
            'importe': item['importe']
        })

    return response(200, {
        'nota': nota,
        'cliente': cliente,
        'direccion_facturacion': dir_fact,
        'direccion_envio': dir_envio,
        'contenidos': contenidos
    })

# ============= DESCARGA DE PDF =============
def download_nota_pdf(rfc, folio):
    key = f"{rfc}/{folio}.pdf"

    try:
        # Verificar que el archivo existe y obtener metadatos
        obj_metadata = s3.head_object(Bucket=BUCKET_NAME, Key=key)
        current_metadata = obj_metadata.get('Metadata', {})

        # Actualizar metadato nota-descargada
        current_metadata['nota-descargada'] = 'true'

        # Obtener el contenido para re-subir con metadatos actualizados
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        pdf_content = obj['Body'].read()

        # Re-subir con metadatos actualizados
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=pdf_content,
            ContentType='application/pdf',
            Metadata=current_metadata
        )

        # Generar URL prefirmada de S3 (válida por 15 minutos)
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': BUCKET_NAME,
                'Key': key,
                'ResponseContentDisposition': f'attachment; filename="{folio}.pdf"'
            },
            ExpiresIn=900  # 15 minutos
        )

        # Retornar redirect a la URL de S3
        return {
            'statusCode': 302,
            'headers': {
                'Location': presigned_url,
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'message': 'Redirigiendo a descarga...', 'url': presigned_url})
        }
    except Exception as e:
        print(f"Error en descarga de PDF: {str(e)}")
        return response(404, {'error': f'PDF no encontrado: {str(e)}'})

# ============= HANDLER PRINCIPAL =============
def lambda_handler(event, context):
    print(f"Event: {json.dumps(event)}")
    print(f"Environment: {ENVIRONMENT}")

    http_method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method'))
    path = event.get('path', event.get('rawPath', ''))

    # Obtener API Gateway URL del contexto
    domain = event.get('requestContext', {}).get('domainName', '')
    stage = event.get('requestContext', {}).get('stage', '')
    api_gateway_url = f"https://{domain}/{stage}" if stage else f"https://{domain}"

    # Parse body
    body = {}
    if event.get('body'):
        try:
            body = json.loads(event['body'])
        except:
            pass

    # Query parameters
    query = event.get('queryStringParameters') or {}

    # Path parameters
    path_params = event.get('pathParameters') or {}
    resource_id = path_params.get('id')

    # Routing
    try:
        # NOTAS DE VENTA
        if '/notas' in path:
            if '/download' in path and http_method == 'GET':
                rfc = query.get('rfc')
                folio = query.get('folio')
                if not rfc or not folio:
                    return response(400, {'error': 'Parámetros rfc y folio son requeridos'})
                return download_nota_pdf(rfc, folio)
            elif http_method == 'POST' and resource_id is None:
                return create_nota_venta(body, api_gateway_url)
            elif http_method == 'GET' and resource_id:
                return get_nota_venta(resource_id)

        return response(404, {'error': 'Endpoint no encontrado'})

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

        # Métrica de error
        try:
            cloudwatch.put_metric_data(
                Namespace='NotasVentaApp',
                MetricData=[{
                    'MetricName': 'ErroresNotasService',
                    'Dimensions': [
                        {'Name': 'Environment', 'Value': ENVIRONMENT},
                        {'Name': 'Service', 'Value': 'notas-service'}
                    ],
                    'Value': 1,
                    'Unit': 'Count',
                    'Timestamp': datetime.now()
                }]
            )
        except:
            pass

        return response(500, {'error': f'Error interno: {str(e)}'})
