FROM continuumio/miniconda3

ENV FLASK_APP app.py
ENV FLASK_ENV development

COPY requirements.txt ./
RUN conda create -n ${FLASK_ENV}FLASK_ENV python=3.8
RUN conda install -n ${FLASK_ENV} requirements.txt
RUN conda init bash
RUN conda activate ${FLASK_ENV}FLASK_ENV}

COPY . .

EXPOSE 5000
CMD ["flask", "run"]
