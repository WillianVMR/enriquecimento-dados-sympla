from flask import Flask, render_template, request, jsonify
import subprocess

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run_etl_script', methods=['POST'])
def run_etl_script():
    # Identifica qual script rodar baseado no JSON recebido
    data = request.get_json()
    script_name = data.get('scriptName')

    # Dicionário para mapear nomes de script para arquivos Python correspondentes
    scripts = {
        'ETL 1': 'controler_ibge.py',
        'ETL 2': 'controler_sympla_pandas.py',
        'ETL 3': 'controler_sympla_spark.py',
    }

    # Seleciona o arquivo de script com base na entrada do usuário
    script_file = scripts.get(script_name)

    if script_file:
        try:
            # Executa o script selecionado
            output = subprocess.check_output(['python', script_file], stderr=subprocess.STDOUT, text=True)
            return jsonify({'message': output})
        except subprocess.CalledProcessError as e:
            # Retorna a saída de erro se o script falhar
            return jsonify({'error': e.output}), 400
    else:
        # Retorna um erro se o nome do script não for reconhecido
        return jsonify({'error': 'Script name not recognized'}), 400

if __name__ == '__main__':
    app.run(debug=True)
