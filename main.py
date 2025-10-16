import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, session, send_file, flash
from datetime import datetime
from functools import wraps
import secrets
import json
from openai import OpenAI

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', secrets.token_hex(32))
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

CLIENTS_CSV = 'data/clients.csv'
SESSIONS_CSV = 'data/sessions.csv'
UPLOAD_FOLDER = 'data/transcripts'

openai_client = None
if os.environ.get('OPENAI_API_KEY'):
    openai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

def init_csv_files():
    os.makedirs('data', exist_ok=True)
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    
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
            'next_session_date', 'fee', 'paid', 'rating', 'transcript',
            'analysis_summary', 'analysis_stress', 'analysis_intervention',
            'analysis_alternatives', 'analysis_plan', 'analysis_emotions',
            'analysis_distortions', 'analysis_resistance'
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

def analyze_transcript(transcript_text):
    if not openai_client:
        return None
    
    try:
        # the newest OpenAI model is "gpt-5" which was released August 7, 2025. do not change this unless explicitly requested by the user
        response = openai_client.chat.completions.create(
            model="gpt-5",
            messages=[
                {
                    "role": "system",
                    "content": "당신은 전문 임상심리상담 수퍼바이저입니다. 상담 축어록을 분석하여 내담자의 심리상태와 상담자의 개입을 전문적으로 평가합니다. 응답은 반드시 JSON 형식으로 제공하며, 각 항목은 한국어로 작성합니다."
                },
                {
                    "role": "user",
                    "content": f"""다음 상담 축어록을 분석하여 JSON 형식으로 응답해주세요:

{transcript_text}

다음 항목들을 분석하여 JSON으로 반환하세요:
{{
  "summary": "전체 이야기 요약 (3-5문장)",
  "stress_factors": "주요 스트레스 요인 분석 (리스트 형태로)",
  "intervention_eval": "상담자 개입에 대한 평가 (장점과 개선점)",
  "alternatives": "더 나은 개입 대안 제안",
  "future_plan": "이후 상담 계획 수립 가이드",
  "emotions": "내담자의 주요 감정 분석",
  "cognitive_distortions": "인지왜곡 패턴 (있다면)",
  "resistance": "저항 패턴 분석 (있다면)"
}}"""
                }
            ],
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        return result
    except Exception as e:
        print(f"AI 분석 오류: {e}")
        import traceback
        traceback.print_exc()
        return None

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

@app.route('/clients/<client_id>/edit', methods=['GET', 'POST'])
@login_required
def client_edit(client_id):
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    
    client_data = clients_df[clients_df['client_id'] == client_id]
    if client_data.empty:
        flash('내담자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('clients_list'))
    
    if request.method == 'POST':
        idx = clients_df[clients_df['client_id'] == client_id].index[0]
        
        clients_df.at[idx, 'name'] = request.form.get('name')
        clients_df.at[idx, 'phone'] = request.form.get('phone')
        clients_df.at[idx, 'email'] = request.form.get('email')
        clients_df.at[idx, 'birth_year'] = request.form.get('birth_year')
        clients_df.at[idx, 'gender'] = request.form.get('gender')
        clients_df.at[idx, 'first_session_date'] = request.form.get('first_session_date')
        clients_df.at[idx, 'status'] = request.form.get('status')
        clients_df.at[idx, 'tags'] = request.form.get('tags')
        clients_df.at[idx, 'notes'] = request.form.get('notes')
        
        clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
        
        flash('내담자 정보가 수정되었습니다.', 'success')
        return redirect(url_for('clients_list'))
    
    client = client_data.fillna('').to_dict('records')[0]
    return render_template('client_edit.html', client=client)

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
            'rating': request.form.get('rating'),
            'transcript': '',
            'analysis_summary': '',
            'analysis_stress': '',
            'analysis_intervention': '',
            'analysis_alternatives': '',
            'analysis_plan': '',
            'analysis_emotions': '',
            'analysis_distortions': '',
            'analysis_resistance': ''
        }
        
        sessions_df = pd.concat([sessions_df, pd.DataFrame([new_session])], ignore_index=True)
        sessions_df.to_csv(SESSIONS_CSV, index=False, encoding='utf-8-sig')
        
        flash('새 회기가 추가되었습니다.', 'success')
        return redirect(url_for('sessions_list'))
    
    clients = clients_df.to_dict('records')
    return render_template('session_form.html', clients=clients)

@app.route('/sessions/<session_id>')
@login_required
def session_detail(session_id):
    sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    
    session_data = sessions_df[sessions_df['session_id'] == session_id]
    if session_data.empty:
        flash('회기를 찾을 수 없습니다.', 'error')
        return redirect(url_for('sessions_list'))
    
    session_data = session_data.merge(
        clients_df[['client_id', 'name']],
        on='client_id',
        how='left'
    )
    
    session_info = session_data.fillna('').to_dict('records')[0]
    return render_template('session_detail.html', session_data=session_info)

@app.route('/sessions/<session_id>/upload-transcript', methods=['POST'])
@login_required
def upload_transcript(session_id):
    transcript_text = ''
    
    if 'transcript_file' in request.files:
        file = request.files['transcript_file']
        if file and file.filename:
            try:
                transcript_text = file.read().decode('utf-8')
            except Exception as e:
                flash(f'파일 읽기 오류: {str(e)}', 'error')
                return redirect(url_for('session_detail', session_id=session_id))
    
    if not transcript_text:
        transcript_text = request.form.get('transcript_text', '').strip()
    
    if not transcript_text:
        flash('축어록 내용이 비어있습니다. 파일을 업로드하거나 텍스트를 직접 입력해주세요.', 'error')
        return redirect(url_for('session_detail', session_id=session_id))
    
    sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
    
    required_columns = [
        'transcript', 'analysis_summary', 'analysis_stress', 'analysis_intervention',
        'analysis_alternatives', 'analysis_plan', 'analysis_emotions',
        'analysis_distortions', 'analysis_resistance'
    ]
    for col in required_columns:
        if col not in sessions_df.columns:
            sessions_df[col] = ''
    
    idx = sessions_df[sessions_df['session_id'] == session_id].index
    
    if len(idx) == 0:
        flash('회기를 찾을 수 없습니다.', 'error')
        return redirect(url_for('sessions_list'))
    
    sessions_df.at[idx[0], 'transcript'] = transcript_text
    
    if openai_client:
        flash('축어록을 업로드하고 AI 분석을 시작합니다...', 'success')
        analysis = analyze_transcript(transcript_text)
        
        if analysis:
            def to_string(value):
                if isinstance(value, (list, dict)):
                    return json.dumps(value, ensure_ascii=False)
                return str(value) if value else ''
            
            sessions_df.at[idx[0], 'analysis_summary'] = to_string(analysis.get('summary', ''))
            sessions_df.at[idx[0], 'analysis_stress'] = to_string(analysis.get('stress_factors', ''))
            sessions_df.at[idx[0], 'analysis_intervention'] = to_string(analysis.get('intervention_eval', ''))
            sessions_df.at[idx[0], 'analysis_alternatives'] = to_string(analysis.get('alternatives', ''))
            sessions_df.at[idx[0], 'analysis_plan'] = to_string(analysis.get('future_plan', ''))
            sessions_df.at[idx[0], 'analysis_emotions'] = to_string(analysis.get('emotions', ''))
            sessions_df.at[idx[0], 'analysis_distortions'] = to_string(analysis.get('cognitive_distortions', ''))
            sessions_df.at[idx[0], 'analysis_resistance'] = to_string(analysis.get('resistance', ''))
            flash('AI 분석이 완료되었습니다!', 'success')
        else:
            flash('축어록이 저장되었지만 AI 분석에 실패했습니다.', 'error')
    else:
        flash('축어록이 저장되었습니다. OpenAI API 키를 설정하면 AI 분석을 사용할 수 있습니다.', 'success')
    
    sessions_df.to_csv(SESSIONS_CSV, index=False, encoding='utf-8-sig')
    return redirect(url_for('session_detail', session_id=session_id))

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
