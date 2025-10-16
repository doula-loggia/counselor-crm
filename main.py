import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, session, send_file, flash
from datetime import datetime
from functools import wraps
import secrets

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', secrets.token_hex(32))

CLIENTS_CSV = 'data/clients.csv'
SESSIONS_CSV = 'data/sessions.csv'

def init_csv_files():
    os.makedirs('data', exist_ok=True)
    
    if not os.path.exists(CLIENTS_CSV):
        clients_df = pd.DataFrame(columns=[
            'client_id', 'name', 'phone', 'email', 'birth_year', 'gender',
            'first_session_date', 'status', 'tags', 'notes'
        ])
        clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
    
    if not os.path.exists(SESSIONS_CSV):
        sessions_df = pd.DataFrame(columns=[
            'session_id', 'client_id', 'date', 'duration_minutes', 'mode',
            'goals', 'interventions', 'notes', 'next_actions',
            'next_session_date', 'fee', 'paid', 'rating'
        ])
        sessions_df.to_csv(SESSIONS_CSV, index=False, encoding='utf-8-sig')

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def generate_client_id():
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    random_suffix = secrets.token_hex(2)
    return f"C-{timestamp}{random_suffix}"

def generate_session_id():
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    random_suffix = secrets.token_hex(2)
    return f"S-{timestamp}{random_suffix}"

@app.route('/')
@login_required
def home():
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
    
    total_clients = len(clients_df)
    active_clients = len(clients_df[clients_df['status'] == 'active'])
    total_sessions = len(sessions_df)
    
    return render_template('home.html', 
                         total_clients=total_clients,
                         active_clients=active_clients,
                         total_sessions=total_sessions)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        password = request.form.get('password')
        secret_password = os.environ.get('SECRET_PASSWORD')
        
        if not secret_password:
            flash('SECRET_PASSWORD 환경변수가 설정되지 않았습니다. Replit Secrets에 SECRET_PASSWORD를 추가해주세요.', 'error')
            return render_template('login.html')
        
        if password == secret_password:
            session['logged_in'] = True
            return redirect(url_for('home'))
        else:
            flash('비밀번호가 올바르지 않습니다.', 'error')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('로그아웃되었습니다.', 'success')
    return redirect(url_for('login'))

@app.route('/clients')
@login_required
def clients_list():
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    search_query = request.args.get('search', '').strip()
    
    if search_query:
        mask = (
            clients_df['name'].str.contains(search_query, case=False, na=False) |
            clients_df['tags'].str.contains(search_query, case=False, na=False)
        )
        clients_df = clients_df[mask]
    
    clients_df = clients_df.fillna('')
    clients = clients_df.to_dict('records')
    
    return render_template('clients_list.html', clients=clients, search_query=search_query)

@app.route('/clients/new', methods=['GET', 'POST'])
@login_required
def client_new():
    if request.method == 'POST':
        clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
        
        new_client = {
            'client_id': generate_client_id(),
            'name': request.form.get('name'),
            'phone': request.form.get('phone'),
            'email': request.form.get('email'),
            'birth_year': request.form.get('birth_year'),
            'gender': request.form.get('gender'),
            'first_session_date': request.form.get('first_session_date'),
            'status': 'active',
            'tags': request.form.get('tags'),
            'notes': request.form.get('notes')
        }
        
        clients_df = pd.concat([clients_df, pd.DataFrame([new_client])], ignore_index=True)
        clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
        
        flash('새 내담자가 추가되었습니다.', 'success')
        return redirect(url_for('clients_list'))
    
    return render_template('client_form.html')

@app.route('/sessions')
@login_required
def sessions_list():
    sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    
    if not sessions_df.empty:
        sessions_df = sessions_df.merge(
            clients_df[['client_id', 'name']],
            on='client_id',
            how='left'
        )
        sessions_df = sessions_df.sort_values('date', ascending=False)
    
    sessions_df = sessions_df.fillna('')
    sessions = sessions_df.to_dict('records')
    
    return render_template('sessions_list.html', sessions=sessions)

@app.route('/sessions/new', methods=['GET', 'POST'])
@login_required
def session_new():
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    
    if request.method == 'POST':
        sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
        
        new_session = {
            'session_id': generate_session_id(),
            'client_id': request.form.get('client_id'),
            'date': request.form.get('date'),
            'duration_minutes': request.form.get('duration_minutes'),
            'mode': request.form.get('mode'),
            'goals': request.form.get('goals'),
            'interventions': request.form.get('interventions'),
            'notes': request.form.get('notes'),
            'next_actions': request.form.get('next_actions'),
            'next_session_date': request.form.get('next_session_date'),
            'fee': request.form.get('fee'),
            'paid': request.form.get('paid'),
            'rating': request.form.get('rating')
        }
        
        sessions_df = pd.concat([sessions_df, pd.DataFrame([new_session])], ignore_index=True)
        sessions_df.to_csv(SESSIONS_CSV, index=False, encoding='utf-8-sig')
        
        flash('새 회기가 추가되었습니다.', 'success')
        return redirect(url_for('sessions_list'))
    
    clients = clients_df.to_dict('records')
    return render_template('session_form.html', clients=clients)

@app.route('/export/clients')
@login_required
def export_clients():
    return send_file(CLIENTS_CSV, 
                    as_attachment=True,
                    download_name=f'clients_{datetime.now().strftime("%Y%m%d")}.csv',
                    mimetype='text/csv')

@app.route('/export/sessions')
@login_required
def export_sessions():
    return send_file(SESSIONS_CSV,
                    as_attachment=True,
                    download_name=f'sessions_{datetime.now().strftime("%Y%m%d")}.csv',
                    mimetype='text/csv')

if __name__ == '__main__':
    init_csv_files()
    app.run(host='0.0.0.0', port=5000, debug=True)
