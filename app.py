from flask import Flask, render_template, request
import subprocess

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run_etl_script', methods=['POST'])
def run_etl_script():
    # Run your ETL script
    try:
        output = subprocess.check_output(['python', 'your_etl_script.py'], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        output = e.output
    
    return output

if __name__ == '__main__':
    app.run(debug=True)
