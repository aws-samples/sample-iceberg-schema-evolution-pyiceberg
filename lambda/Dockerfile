FROM public.ecr.aws/lambda/python:3.12
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD pidof nginx || exit 1
# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}
# Install dependencies
RUN pip install -r requirements.txt

# Add user and switch to it
RUN echo "appuser:x:1000:1000::/home/appuser:/bin/bash" >> /etc/passwd && \
    echo "appuser:x:1000:" >> /etc/group && \
    mkdir -p /home/appuser && \
    chown 1000:1000 /home/appuser

USER appuser
# Copy function code
COPY . ${LAMBDA_TASK_ROOT}
# Set the CMD to your handler
CMD [ "handler.handler" ]

