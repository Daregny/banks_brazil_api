from flask import Flask
from flask_restx import Api, Resource, fields
from werkzeug.middleware.proxy_fix import ProxyFix
from apscheduler.schedulers.background import BackgroundScheduler
from api.csv_processing import reader, select_api, select_banks_api
from os import environ
import os.path
import shutil
import wget

source = r'./api/ParticipantesSTRport.csv'
destination = r'./api/backup/ParticipantesSTRport.csv'
destination_db = r'./api/backup/banks_dump.sql'

def download_csv():
    url = 'http://www.bcb.gov.br/pom/spb/estatistica/port/ParticipantesSTRport.csv'
    if os.path.isfile(source):
        shutil.move(source, destination)
        wget.download(url, source)
        reader(source, destination_db)
    else:
        wget.download(url, source)
        reader(source, destination_db)


scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(func=download_csv, trigger='cron', year='*', month='*', day='last')
#scheduler.add_job(func=download_csv, trigger="interval", seconds=60)
scheduler.start()


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
api = Api(app, version='1.0', title='Banks API',
    description='Api to consult the code of active banks on the second date of the Central Bank of Brazil. https://www.bcb.gov.br/estabilidadefinanceira/relacao_instituicoes_funcionamento',
)

ns = api.namespace('api/v1/banks', description='Brazilian banks with their code according to Central Bank of Brazil')

bank = api.model('Banks', {
    'code': fields.String(readonly=True, description='Bank code'),
    'name': fields.String(readonly=True, description='Bank name')
})


class Bank(object):
    def __init__(self):
        self.counter = 0
        self.banks = []

    def get(self, bank_code):
        data = select_api(bank_code)
        return data
        api.abort(404, "Bank {} doesn't exist".format(bank_code))


DAO = Bank()

@ns.route('/<string:bank_code>')
@ns.response(200, 'Ok')
@ns.response(404, 'Bank not found')
@ns.param('bank_code', 'The Bank code for the search')
class Bank(Resource):
    @ns.doc('get_bank')
    def get(self, bank_code):
        return DAO.get(bank_code)


class BankList(object):
    def __init__(self):
        self.counter = 0
        self.banks = []

    def get(self):
        data = select_banks_api()
        return data
        api.abort(404, "Bank {} doesn't exist".format(bank_code))


DAOT = BankList()


@ns.route('/')
@ns.response(200, 'Ok')
@ns.response(404, 'Bank not found')
class Bank(Resource):
    @ns.doc('get_banks')
    def get(self,):
        return DAOT.get()


if __name__ == '__main__':
    SERVER_HOST = environ.get('SERVER_HOST', 'localhost')
    app.run(host=SERVER_HOST, port=5500, debug=(not environ.get('ENV') == 'PRODUCTION'), threaded=True)
    app.run(debug=True)