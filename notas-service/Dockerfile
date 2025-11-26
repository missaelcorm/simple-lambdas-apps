FROM public.ecr.aws/lambda/python:3.11

# Copiar requirements e instalar dependencias
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

# Copiar código de la función
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Comando para ejecutar la función
CMD [ "lambda_function.lambda_handler" ]
