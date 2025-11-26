import json
import boto3
import os
from datetime import datetime

# Variables de entorno
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'local')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')

# Clientes AWS
sns = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')

def enviar_metrica(metric_name, value, unit='Count', dimensions=None):
    """Helper para enviar métricas a CloudWatch"""
    if dimensions is None:
        dimensions = []

    # Agregar dimensiones estándar
    standard_dimensions = [
        {'Name': 'Environment', 'Value': ENVIRONMENT},
        {'Name': 'Service', 'Value': 'notifications-service'}
    ]
    all_dimensions = standard_dimensions + dimensions

    try:
        cloudwatch.put_metric_data(
            Namespace='NotasVentaApp',
            MetricData=[{
                'MetricName': metric_name,
                'Dimensions': all_dimensions,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.now()
            }]
        )
        print(f"Métrica enviada: {metric_name} = {value} {unit}")
    except Exception as e:
        print(f"Error enviando métrica {metric_name}: {str(e)}")

def enviar_notificacion_sns(destinatario_email, folio, rfc, download_url):
    """Envía notificación usando Amazon SNS"""
    start_time = datetime.now()

    subject = f"Nueva Nota de Venta - Folio {folio}"

    # Mensaje en texto plano (SNS email no soporta HTML directamente)
    message = f"""
NOTA DE VENTA GENERADA

Estimado cliente,

Se ha generado una nueva nota de venta con los siguientes datos:

Folio: {folio}
RFC: {rfc}
Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}

Para descargar su nota de venta, acceda al siguiente enlace:
{download_url}

Gracias por su preferencia.

---
Este es un correo automático, por favor no responda a este mensaje.
Ambiente: {ENVIRONMENT.upper()}
    """

    try:
        # Publicar mensaje al tópico SNS
        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message,
            MessageAttributes={
                'folio': {'DataType': 'String', 'StringValue': folio},
                'rfc': {'DataType': 'String', 'StringValue': rfc},
                'email': {'DataType': 'String', 'StringValue': destinatario_email},
                'environment': {'DataType': 'String', 'StringValue': ENVIRONMENT}
            }
        )

        # Calcular tiempo de ejecución
        end_time = datetime.now()
        execution_time_ms = (end_time - start_time).total_seconds() * 1000

        # Enviar métricas de éxito
        enviar_metrica('NotificacionesEnviadas', 1)
        enviar_metrica('NotificacionesExitosas', 1)
        enviar_metrica('TiempoEnvioNotificacion', execution_time_ms, 'Milliseconds')

        # Métricas por código de respuesta (2xx)
        enviar_metrica('HTTP_2xx', 1)

        print(f"Notificación SNS enviada exitosamente. MessageId: {response['MessageId']}")
        print(f"Tiempo de ejecución: {execution_time_ms:.2f}ms")

        return {
            'success': True,
            'message_id': response['MessageId'],
            'execution_time_ms': execution_time_ms
        }

    except sns.exceptions.InvalidParameterException as e:
        print(f"Parámetros inválidos: {str(e)}")
        enviar_metrica('NotificacionesEnviadas', 1)
        enviar_metrica('NotificacionesFallidas', 1)
        enviar_metrica('HTTP_4xx', 1)
        return {'success': False, 'error': f'Parámetros inválidos: {str(e)}', 'status_code': 400}

    except sns.exceptions.NotFoundException as e:
        print(f"Tópico SNS no encontrado: {str(e)}")
        enviar_metrica('NotificacionesEnviadas', 1)
        enviar_metrica('NotificacionesFallidas', 1)
        enviar_metrica('HTTP_4xx', 1)
        return {'success': False, 'error': f'Tópico SNS no encontrado: {str(e)}', 'status_code': 404}

    except Exception as e:
        print(f"Error enviando notificación SNS: {str(e)}")
        enviar_metrica('NotificacionesEnviadas', 1)
        enviar_metrica('NotificacionesFallidas', 1)
        enviar_metrica('HTTP_5xx', 1)
        return {'success': False, 'error': str(e), 'status_code': 500}

def lambda_handler(event, context):
    """
    Handler que procesa eventos SNS y envía notificaciones.
    Puede ser invocado de dos formas:
    1. Via SNS (evento de nota de venta creada desde notas-service)
    2. Via HTTP (para testing o invocación directa)
    """
    print(f"Event: {json.dumps(event)}")
    print(f"Environment: {ENVIRONMENT}")

    # Timestamp de inicio para métrica de tiempo total
    start_time = datetime.now()

    try:
        # Determinar tipo de invocación
        if 'Records' in event and len(event['Records']) > 0:
            # ========== Invocación via SNS ==========
            for record in event['Records']:
                if record.get('EventSource') == 'aws:sns':
                    sns_message = record['Sns']['Message']

                    # Parsear mensaje
                    try:
                        message_data = json.loads(sns_message)
                    except:
                        # Si no es JSON, asumir que es mensaje simple
                        print(f"Mensaje SNS recibido (texto plano): {sns_message}")
                        enviar_metrica('MensajesNoJSON', 1)
                        continue

                    # Extraer datos del mensaje
                    email = message_data.get('email')
                    folio = message_data.get('folio')
                    rfc = message_data.get('rfc')
                    api_gateway_url = message_data.get('api_gateway_url', '')

                    if not all([email, folio, rfc]):
                        print("Mensaje SNS incompleto, faltan campos requeridos")
                        enviar_metrica('MensajesInvalidos', 1)
                        enviar_metrica('HTTP_4xx', 1)
                        continue

                    # Construir URL de descarga
                    download_url = f"{api_gateway_url}/notas/download?rfc={rfc}&folio={folio}"

                    # Enviar notificación
                    resultado = enviar_notificacion_sns(email, folio, rfc, download_url)

                    print(f"Resultado envío notificación: {resultado}")

            # Para invocaciones SNS, siempre retornar éxito
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Procesamiento SNS completado'})
            }

        else:
            # ========== Invocación HTTP directa (para testing) ==========
            body = {}
            if event.get('body'):
                try:
                    body = json.loads(event['body'])
                except:
                    body = event
            else:
                body = event

            email = body.get('email')
            folio = body.get('folio')
            rfc = body.get('rfc')
            api_gateway_url = body.get('api_gateway_url', 'https://api.example.com')

            if not all([email, folio, rfc]):
                enviar_metrica('RequestsInvalidos', 1)
                enviar_metrica('HTTP_4xx', 1)
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({'error': 'Campos requeridos: email, folio, rfc'})
                }

            # Construir URL de descarga
            download_url = f"{api_gateway_url}/notas/download?rfc={rfc}&folio={folio}"

            # Enviar notificación
            resultado = enviar_notificacion_sns(email, folio, rfc, download_url)

            # Calcular tiempo total de ejecución
            end_time = datetime.now()
            total_execution_time_ms = (end_time - start_time).total_seconds() * 1000

            # Enviar métrica de tiempo total
            enviar_metrica('TiempoTotalEjecucion', total_execution_time_ms, 'Milliseconds')

            if resultado['success']:
                return {
                    'statusCode': 200,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'message': 'Notificación enviada exitosamente',
                        'message_id': resultado['message_id'],
                        'execution_time_ms': resultado['execution_time_ms'],
                        'total_execution_time_ms': total_execution_time_ms
                    })
                }
            else:
                status_code = resultado.get('status_code', 500)
                return {
                    'statusCode': status_code,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'Error enviando notificación',
                        'details': resultado['error']
                    })
                }

    except Exception as e:
        print(f"Error en lambda_handler: {str(e)}")
        import traceback
        traceback.print_exc()

        # Métrica de error general
        enviar_metrica('ErroresNotificationsService', 1)
        enviar_metrica('HTTP_5xx', 1)

        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': f'Error interno: {str(e)}'})
        }
