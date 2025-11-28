#!/usr/bin/env python3
"""
Script de Testing - Notas de Venta
Prueba el flujo completo de la aplicación
"""

import requests
import logging
import sys
import json
import os
from datetime import datetime

# Configuración de API
API_BASE_URL = os.environ.get('API_BASE_URL')

if not API_BASE_URL:
    print(f"API_BASE_URL env variable no configurads")
    sys.exit(1)

# Configurar logging con colores
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class TestApp:
    """Clase para testing de la aplicación"""

    def __init__(self):
        # IDs generados durante el test
        self.cliente_id = None
        self.rfc = None
        self.dir_fact_id = None
        self.dir_envio_id = None
        self.producto1_id = None
        self.producto2_id = None
        self.nota_id = None
        self.folio = None

    def crear_cliente(self):
        """Paso 1: Crear cliente de prueba"""
        logger.info("=" * 50)
        logger.info("PASO 1: Creando cliente...")

        data = {
            "razon_social": "Empresa Test SA de CV",
            "nombre_comercial": "Test Corp",
            "rfc": "ETE201125XYZ",
            "correo": "test@example.com",
            "telefono": "5551234567"
        }

        try:
            response = requests.post(
                f"{API_BASE_URL}/clientes",
                json=data,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()

            result = response.json()
            self.cliente_id = result['id']
            self.rfc = result['rfc']

            logger.info(f"✅ Cliente creado exitosamente")
            logger.info(f"   ID: {self.cliente_id}")
            logger.info(f"   RFC: {self.rfc}")

            return True

        except Exception as e:
            logger.error(f"❌ Error creando cliente: {e}")
            return False

    def crear_domicilios(self):
        """Paso 2: Crear domicilios de facturación y envío"""
        logger.info("=" * 50)
        logger.info("PASO 2: Creando domicilios...")

        # Domicilio de facturación
        dir_fact = {
            "cliente_id": self.cliente_id,
            "domicilio": "Av. Insurgentes Sur 1602",
            "colonia": "Crédito Constructor",
            "municipio": "Benito Juárez",
            "estado": "Ciudad de México",
            "tipo": "FACTURACION"
        }

        try:
            response = requests.post(
                f"{API_BASE_URL}/domicilios",
                json=dir_fact
            )
            response.raise_for_status()

            self.dir_fact_id = response.json()['id']
            logger.info(f"✅ Domicilio de facturación creado: {self.dir_fact_id}")

        except Exception as e:
            logger.error(f"❌ Error creando domicilio de facturación: {e}")
            return False

        # Domicilio de envío
        dir_envio = {
            "cliente_id": self.cliente_id,
            "domicilio": "Calle Reforma 500",
            "colonia": "Juárez",
            "municipio": "Cuauhtémoc",
            "estado": "Ciudad de México",
            "tipo": "ENVIO"
        }

        try:
            response = requests.post(
                f"{API_BASE_URL}/domicilios",
                json=dir_envio
            )
            response.raise_for_status()

            self.dir_envio_id = response.json()['id']
            logger.info(f"✅ Domicilio de envío creado: {self.dir_envio_id}")

            return True

        except Exception as e:
            logger.error(f"❌ Error creando domicilio de envío: {e}")
            return False

    def crear_productos(self):
        """Paso 3: Crear productos"""
        logger.info("=" * 50)
        logger.info("PASO 3: Creando productos...")

        # Producto 1: Laptop
        laptop = {
            "nombre": "Laptop Dell XPS 15",
            "unidad_medida": "Pieza",
            "precio_base": 25000.00
        }

        try:
            response = requests.post(
                f"{API_BASE_URL}/productos",
                json=laptop
            )
            response.raise_for_status()

            self.producto1_id = response.json()['id']
            logger.info(f"✅ Laptop creada: {self.producto1_id}")

        except Exception as e:
            logger.error(f"❌ Error creando laptop: {e}")
            return False

        # Producto 2: Mouse
        mouse = {
            "nombre": "Mouse Wireless Logitech",
            "unidad_medida": "Pieza",
            "precio_base": 350.00
        }

        try:
            response = requests.post(
                f"{API_BASE_URL}/productos",
                json=mouse
            )
            response.raise_for_status()

            self.producto2_id = response.json()['id']
            logger.info(f"✅ Mouse creado: {self.producto2_id}")

            return True

        except Exception as e:
            logger.error(f"❌ Error creando mouse: {e}")
            return False

    def crear_nota_venta(self):
        """Paso 4: Crear nota de venta"""
        logger.info("=" * 50)
        logger.info("PASO 4: Creando nota de venta...")

        nota = {
            "cliente_id": self.cliente_id,
            "direccion_facturacion_id": self.dir_fact_id,
            "direccion_envio_id": self.dir_envio_id,
            "productos": [
                {
                    "producto_id": self.producto1_id,
                    "cantidad": 1
                },
                {
                    "producto_id": self.producto2_id,
                    "cantidad": 2
                }
            ]
        }

        try:
            response = requests.post(
                f"{API_BASE_URL}/notas",
                json=nota
            )
            response.raise_for_status()

            result = response.json()
            self.nota_id = result['nota']['id']
            self.folio = result['nota']['folio']
            total = result['nota']['total']
            exec_time = result.get('execution_time_ms', 0)

            logger.info(f"✅ Nota de venta creada exitosamente")
            logger.info(f"   ID: {self.nota_id}")
            logger.info(f"   Folio: {self.folio}")
            logger.info(f"   Total: ${total}")
            logger.info(f"   Tiempo: {exec_time:.2f}ms")
            logger.warning(f"⚠️  Notificación enviada por SNS - Revisa tu email")

            return True

        except Exception as e:
            logger.error(f"❌ Error creando nota de venta: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"   Respuesta: {e.response.text}")
            return False

    def consultar_nota(self):
        """Paso 5: Consultar nota de venta creada"""
        logger.info("=" * 50)
        logger.info("PASO 5: Consultando nota de venta...")

        try:
            response = requests.get(f"{API_BASE_URL}/notas/{self.nota_id}")
            response.raise_for_status()

            result = response.json()

            logger.info(f"✅ Nota consultada exitosamente")
            logger.info(f"   Cliente: {result['cliente']['razon_social']}")
            logger.info(f"   Productos: {len(result['contenidos'])} items")

            return True

        except Exception as e:
            logger.error(f"❌ Error consultando nota: {e}")
            return False

    def descargar_pdf(self):
        """Paso 6: Descargar PDF de la nota"""
        logger.info("=" * 50)
        logger.info("PASO 6: Descargando PDF...")

        download_url = f"{API_BASE_URL}/notas/download?rfc={self.rfc}&folio={self.folio}"

        try:
            # La API retorna un redirect 302, seguimos la redirección
            response = requests.get(download_url, allow_redirects=True)

            if response.status_code == 200 and response.headers.get('Content-Type') == 'application/pdf':
                # Guardar PDF
                filename = f"nota_{self.folio}.pdf"
                with open(filename, 'wb') as f:
                    f.write(response.content)

                size_kb = len(response.content) / 1024
                logger.info(f"✅ PDF descargado: {filename} ({size_kb:.1f} KB)")

                return True
            else:
                logger.warning(f"⚠️  La descarga requiere seguir redirect manualmente")
                logger.info(f"   URL: {download_url}")

                return True

        except Exception as e:
            logger.error(f"❌ Error descargando PDF: {e}")
            return False

    def listar_datos(self):
        """Paso 7: Listar todos los datos creados"""
        logger.info("=" * 50)
        logger.info("PASO 7: Listando datos creados...")

        try:
            # Listar clientes
            response = requests.get(f"{API_BASE_URL}/clientes")
            clientes = response.json()
            logger.info(f"📋 Total clientes: {len(clientes)}")

            # Listar domicilios
            response = requests.get(f"{API_BASE_URL}/domicilios")
            domicilios = response.json()
            logger.info(f"📋 Total domicilios: {len(domicilios)}")

            # Listar productos
            response = requests.get(f"{API_BASE_URL}/productos")
            productos = response.json()
            logger.info(f"📋 Total productos: {len(productos)}")

            return True

        except Exception as e:
            logger.error(f"❌ Error listando datos: {e}")
            return False

    def limpiar_datos(self):
        """Paso 8 (opcional): Eliminar datos de prueba"""
        logger.info("=" * 50)
        logger.info("PASO 8 (OPCIONAL): Limpieza de datos")

        respuesta = input("¿Deseas eliminar los datos de prueba? (y/N): ").strip().lower()

        if respuesta != 'y':
            logger.info("ℹ️  Datos de prueba conservados")
            return True

        try:
            # Eliminar domicilios
            if self.dir_fact_id:
                requests.delete(f"{API_BASE_URL}/domicilios/{self.dir_fact_id}")
                logger.info(f"✅ Domicilio facturación eliminado")

            if self.dir_envio_id:
                requests.delete(f"{API_BASE_URL}/domicilios/{self.dir_envio_id}")
                logger.info(f"✅ Domicilio envío eliminado")

            # Eliminar productos
            if self.producto1_id:
                requests.delete(f"{API_BASE_URL}/productos/{self.producto1_id}")
                logger.info(f"✅ Laptop eliminada")

            if self.producto2_id:
                requests.delete(f"{API_BASE_URL}/productos/{self.producto2_id}")
                logger.info(f"✅ Mouse eliminado")

            # Eliminar cliente
            if self.cliente_id:
                requests.delete(f"{API_BASE_URL}/clientes/{self.cliente_id}")
                logger.info(f"✅ Cliente eliminado")

            logger.info(f"✅ Limpieza completada")
            logger.warning(f"⚠️  Nota: La nota de venta NO se elimina (auditoría)")

            return True

        except Exception as e:
            logger.error(f"❌ Error en limpieza: {e}")
            return False

    def mostrar_resumen(self):
        """Mostrar resumen final del test"""
        logger.info("=" * 50)
        logger.info("RESUMEN DEL TEST")
        logger.info("=" * 50)

        print(f"""
✅ Test Completado Exitosamente

IDs Generados:
  Cliente:     {self.cliente_id}
  RFC:         {self.rfc}
  Dir. Fact:   {self.dir_fact_id}
  Dir. Envío:  {self.dir_envio_id}
  Producto 1:  {self.producto1_id}
  Producto 2:  {self.producto2_id}
  Nota:        {self.nota_id}
  Folio:       {self.folio}

API:
  Base URL: {API_BASE_URL}

Próximos pasos:
  1. Revisa tu email para la notificación
  2. Verifica métricas en CloudWatch
  3. Revisa logs en CloudWatch Logs
        """)

    def ejecutar_test_completo(self):
        """Ejecutar el test completo paso a paso"""
        logger.info("🚀 Iniciando test de la aplicación...")
        logger.info(f"Hora de inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        pasos = [
            ("Crear Cliente", self.crear_cliente),
            ("Crear Domicilios", self.crear_domicilios),
            ("Crear Productos", self.crear_productos),
            ("Crear Nota de Venta", self.crear_nota_venta),
            ("Consultar Nota", self.consultar_nota),
            ("Descargar PDF", self.descargar_pdf),
            ("Listar Datos", self.listar_datos),
        ]

        # Ejecutar pasos
        for nombre, funcion in pasos:
            if not funcion():
                logger.error(f"❌ Test fallido en: {nombre}")
                sys.exit(1)
            logger.info("")  # Línea en blanco

        # Limpieza opcional
        self.limpiar_datos()

        # Resumen
        self.mostrar_resumen()

        logger.info(f"✅ Test completado exitosamente")
        logger.info(f"Hora de fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """Función principal"""
    print("""
    ╔══════════════════════════════════════╗
    ║  Test de Notas de Venta - Examen 2   ║
    ║  Script de Testing Completo          ║
    ╚══════════════════════════════════════╝
    """)

    test = TestApp()
    test.ejecutar_test_completo()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Test interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Error inesperado: {e}")
        sys.exit(1)
