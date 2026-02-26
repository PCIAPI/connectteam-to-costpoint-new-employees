FROM public.ecr.aws/lambda/python:3.11

COPY ./ ./

ARG GH_ACCESS_TOKEN

RUN yum update -y
RUN yum install git -y
RUN git config --global url."https://oauth2:${GH_ACCESS_TOKEN}@github.com/".insteadOf "https://github.com/"
RUN pip3 install -r ./requirements.txt

CMD [ "main.lambda_handler" ]
