# Hugging Face Docker Space.
#
# HF runs the container as user ID 1000, so the user is created before anything
# is copied in and every COPY sets --chown=user. Files written anywhere in the
# container are lost on restart; app.py uploads them to a Dataset repo instead.

FROM python:3.12-slim

RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH
WORKDIR $HOME/app

COPY --chown=user requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY --chown=user . $HOME/app

ENV CAPTURE_OUTPUT_DIR=/home/user/app/raw \
    PYTHONUNBUFFERED=1

EXPOSE 7860

CMD ["python", "app.py"]
