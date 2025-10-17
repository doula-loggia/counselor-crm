import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, session, send_file, flash
from datetime import datetime
from functools import wraps
import secrets
import json
from openai import OpenAI
import PyPDF2
from docx import Document
import olefile
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', secrets.token_hex(32))
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = False
app.config['SESSION_COOKIE_HTTPONLY'] = True

@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response

CLIENTS_CSV = 'data/clients.csv'
SESSIONS_CSV = 'data/sessions.csv'
UPLOAD_FOLDER = 'data/transcripts'
AUDIO_FOLDER = 'data/audio'
PSYCH_TEST_FOLDER = 'data/psychological_tests'
ALLOWED_PSYCH_TEST_EXTENSIONS = {'.pdf', '.doc', '.docx', '.txt', '.jpg', '.jpeg', '.png'}

openai_client = None
if os.environ.get('OPENAI_API_KEY'):
    openai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

def init_csv_files():
    os.makedirs('data', exist_ok=True)
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(AUDIO_FOLDER, exist_ok=True)
    os.makedirs(PSYCH_TEST_FOLDER, exist_ok=True)
    
    if not os.path.exists(CLIENTS_CSV):
        clients_df = pd.DataFrame(columns=[
            'client_id', 'name', 'phone', 'email', 'birth_year', 'gender',
            'first_session_date', 'status', 'tags', 'notes', 'medical_history',
            'counseling_history', 'psychological_test_file'
        ])
        clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
    
    if not os.path.exists(SESSIONS_CSV):
        sessions_df = pd.DataFrame(columns=[
            'session_id', 'client_id', 'date', 'duration_minutes', 'mode',
            'goals', 'interventions', 'tags', 'notes', 'next_actions',
            'next_session_date', 'fee', 'paid', 'payment_method', 'rating', 'transcript',
            'audio_file', 'analysis_summary', 'analysis_stress', 'analysis_intervention',
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

def extract_text_from_file(file):
    filename = file.filename.lower()
    
    try:
        if filename.endswith('.txt'):
            return file.read().decode('utf-8')
        
        elif filename.endswith('.pdf'):
            pdf_reader = PyPDF2.PdfReader(file)
            text = ''
            for page in pdf_reader.pages:
                text += page.extract_text() + '\n'
            return text.strip()
        
        elif filename.endswith('.docx'):
            doc = Document(file)
            text = ''
            for paragraph in doc.paragraphs:
                text += paragraph.text + '\n'
            return text.strip()
        
        elif filename.endswith('.hwp'):
            if olefile.isOleFile(file):
                ole = olefile.OleFileIO(file)
                if ole.exists('PrvText'):
                    stream = ole.openstream('PrvText')
                    data = stream.read()
                    text = data.decode('utf-16', errors='ignore')
                    return text.strip()
                else:
                    ole.close()
                    return None
            else:
                return None
        
        else:
            return None
            
    except Exception as e:
        print(f"파일 읽기 오류: {e}")
        return None

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
                    "content": "당신은 전문 임상심리상담 수퍼바이저입니다. 상담 축어록을 분석하여 내담자의 심리상태와 상담자의 개입을 전문적으로 평가합니다. 응답은 반드시 JSON 형식으로 제공하며, 각 항목은 한국어로 작성합니다. 모든 분석 내용은 자연스러운 문장으로 작성하고, 세부 항목이 여러 개인 경우 번호를 매겨 구분해주세요."
                },
                {
                    "role": "user",
                    "content": f"""다음 상담 축어록을 분석하여 JSON 형식으로 응답해주세요:

{transcript_text}

다음 항목들을 분석하여 JSON으로 반환하세요. 각 항목은 자연스러운 문장으로 작성하고, 세부 항목은 번호를 매겨 구분하세요:

{{
  "summary": "전체 이야기를 3-5문장으로 요약. 각 문장은 줄바꿈(\\n\\n)으로 구분.",
  "stress_factors": "주요 스트레스 요인을 번호를 매겨 설명.\\n예시:\\n1. 첫 번째 스트레스 요인에 대한 구체적 설명\\n2. 두 번째 스트레스 요인에 대한 구체적 설명\\n3. 세 번째 스트레스 요인에 대한 구체적 설명",
  "intervention_eval": "상담자 개입을 평가하는 문장들. 긍정적 측면과 개선이 필요한 측면을 번호를 매겨 설명.\\n예시:\\n\\n긍정적 측면:\\n1. 첫 번째 긍정적 개입에 대한 설명\\n2. 두 번째 긍정적 개입에 대한 설명\\n\\n개선이 필요한 측면:\\n1. 첫 번째 개선점에 대한 설명\\n2. 두 번째 개선점에 대한 설명",
  "alternatives": "더 나은 개입 대안을 번호를 매겨 구체적으로 설명.\\n예시:\\n1. 첫 번째 대안 개입 방법과 그 효과\\n2. 두 번째 대안 개입 방법과 그 효과\\n3. 세 번째 대안 개입 방법과 그 효과",
  "future_plan": "이후 상담 계획을 단계별로 번호를 매겨 제시.\\n예시:\\n\\n단기 목표:\\n1. 첫 번째 단기 목표 설명\\n2. 두 번째 단기 목표 설명\\n\\n중장기 목표:\\n1. 첫 번째 중장기 목표 설명\\n2. 두 번째 중장기 목표 설명\\n\\n권장 개입 기법:\\n1. 첫 번째 기법과 적용 방법\\n2. 두 번째 기법과 적용 방법",
  "emotions": "내담자의 주요 감정을 번호를 매겨 구체적으로 설명.\\n예시:\\n1. 첫 번째 감정과 그것이 표현된 맥락\\n2. 두 번째 감정과 그것이 표현된 맥락\\n3. 세 번째 감정과 그것이 표현된 맥락",
  "cognitive_distortions": "발견된 인지왜곡 패턴을 번호를 매겨 설명. 각 패턴마다 예시 포함.\\n있다면 번호를 매긴 목록으로, 없으면 '특별한 인지왜곡 패턴이 관찰되지 않았습니다.'",
  "resistance": "관찰된 저항 패턴을 번호를 매겨 설명. 각 패턴마다 구체적 예시 포함.\\n있다면 번호를 매긴 목록으로, 없으면 '명확한 저항 패턴이 관찰되지 않았습니다.'"
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
        
        print(f"DEBUG - Login attempt: password='{password}', secret='{secret_password}'")
        
        if not secret_password:
            print("DEBUG - No SECRET_PASSWORD found!")
            flash('SECRET_PASSWORD 환경변수가 설정되지 않았습니다. Replit Secrets에 SECRET_PASSWORD를 추가해주세요.', 'error')
            return render_template('login.html')
        
        if password == secret_password:
            print("DEBUG - Login successful!")
            session['logged_in'] = True
            return redirect(url_for('home'))
        else:
            print(f"DEBUG - Login failed! password != secret_password")
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
    status_filter = request.args.get('status', '').strip()
    
    if status_filter:
        clients_df = clients_df[clients_df['status'] == status_filter]
    
    if search_query:
        mask = (
            clients_df['name'].str.contains(search_query, case=False, na=False) |
            clients_df['tags'].str.contains(search_query, case=False, na=False)
        )
        clients_df = clients_df[mask]
    
    clients_df = clients_df.fillna('')
    clients = clients_df.to_dict('records')
    
    return render_template('clients_list.html', clients=clients, search_query=search_query, status_filter=status_filter)

@app.route('/clients/new', methods=['GET', 'POST'])
@login_required
def client_new():
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    
    if request.method == 'POST':
        client_id = generate_client_id()
        
        psychological_test_filename = ''
        if 'psychological_test' in request.files:
            file = request.files['psychological_test']
            if file and file.filename:
                filename = secure_filename(file.filename)
                file_ext = os.path.splitext(filename)[1].lower()
                
                if file_ext not in ALLOWED_PSYCH_TEST_EXTENSIONS:
                    flash(f'허용되지 않는 파일 형식입니다. PDF, DOC, DOCX, TXT, JPG, PNG 파일만 업로드 가능합니다.', 'error')
                    return redirect(url_for('client_new'))
                
                unique_filename = f"{client_id}_{filename}"
                file_path = os.path.join(PSYCH_TEST_FOLDER, unique_filename)
                file.save(file_path)
                psychological_test_filename = unique_filename
        
        new_client = {
            'client_id': client_id,
            'name': request.form.get('name'),
            'phone': request.form.get('phone'),
            'email': request.form.get('email'),
            'birth_year': request.form.get('birth_year'),
            'gender': request.form.get('gender'),
            'first_session_date': request.form.get('first_session_date'),
            'status': 'active',
            'tags': request.form.get('tags'),
            'notes': request.form.get('notes'),
            'medical_history': request.form.get('medical_history'),
            'counseling_history': request.form.get('counseling_history'),
            'psychological_test_file': psychological_test_filename
        }
        
        clients_df = pd.concat([clients_df, pd.DataFrame([new_client])], ignore_index=True)
        clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
        
        flash('새 내담자가 추가되었습니다.', 'success')
        return redirect(url_for('clients_list'))
    
    all_tags = set()
    for tags_str in clients_df['tags'].dropna():
        if tags_str:
            tags = [tag.strip() for tag in str(tags_str).split(',')]
            all_tags.update(tags)
    
    existing_tags = sorted(list(all_tags))
    return render_template('client_form.html', existing_tags=existing_tags)

@app.route('/clients/<client_id>')
@login_required
def client_detail(client_id):
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
    
    client_data = clients_df[clients_df['client_id'] == client_id]
    if client_data.empty:
        flash('내담자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('clients_list'))
    
    client_info = client_data.fillna('').to_dict('records')[0]
    
    client_sessions = sessions_df[sessions_df['client_id'] == client_id]
    if not client_sessions.empty:
        client_sessions = client_sessions.sort_values('date', ascending=False)
    
    client_sessions = client_sessions.fillna('')
    sessions = client_sessions.to_dict('records')
    
    return render_template('client_detail.html', client=client_info, sessions=sessions)

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
        clients_df.at[idx, 'medical_history'] = request.form.get('medical_history')
        clients_df.at[idx, 'counseling_history'] = request.form.get('counseling_history')
        
        if 'psychological_test' in request.files:
            file = request.files['psychological_test']
            if file and file.filename:
                filename = secure_filename(file.filename)
                file_ext = os.path.splitext(filename)[1].lower()
                
                if file_ext not in ALLOWED_PSYCH_TEST_EXTENSIONS:
                    flash(f'허용되지 않는 파일 형식입니다. PDF, DOC, DOCX, TXT, JPG, PNG 파일만 업로드 가능합니다.', 'error')
                    return redirect(url_for('client_edit', client_id=client_id))
                
                unique_filename = f"{client_id}_{filename}"
                file_path = os.path.join(PSYCH_TEST_FOLDER, unique_filename)
                file.save(file_path)
                clients_df.at[idx, 'psychological_test_file'] = unique_filename
        
        clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
        
        flash('내담자 정보가 수정되었습니다.', 'success')
        return redirect(url_for('clients_list'))
    
    client = client_data.fillna('').to_dict('records')[0]
    
    all_tags = set()
    for tags_str in clients_df['tags'].dropna():
        if tags_str:
            tags = [tag.strip() for tag in str(tags_str).split(',')]
            all_tags.update(tags)
    
    existing_tags = sorted(list(all_tags))
    return render_template('client_edit.html', client=client, existing_tags=existing_tags)

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
            'tags': request.form.get('tags'),
            'notes': request.form.get('notes'),
            'next_actions': request.form.get('next_actions'),
            'next_session_date': request.form.get('next_session_date'),
            'fee': request.form.get('fee'),
            'paid': request.form.get('paid'),
            'payment_method': request.form.get('payment_method'),
            'rating': request.form.get('rating'),
            'transcript': '',
            'audio_file': '',
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

@app.route('/sessions/<session_id>/edit', methods=['GET', 'POST'])
@login_required
def session_edit(session_id):
    sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    
    session_data = sessions_df[sessions_df['session_id'] == session_id]
    if session_data.empty:
        flash('회기를 찾을 수 없습니다.', 'error')
        return redirect(url_for('sessions_list'))
    
    if request.method == 'POST':
        idx = sessions_df[sessions_df['session_id'] == session_id].index[0]
        
        sessions_df.at[idx, 'client_id'] = request.form.get('client_id')
        sessions_df.at[idx, 'date'] = request.form.get('date')
        sessions_df.at[idx, 'duration_minutes'] = request.form.get('duration_minutes')
        sessions_df.at[idx, 'mode'] = request.form.get('mode')
        sessions_df.at[idx, 'goals'] = request.form.get('goals')
        sessions_df.at[idx, 'interventions'] = request.form.get('interventions')
        sessions_df.at[idx, 'tags'] = request.form.get('tags')
        sessions_df.at[idx, 'notes'] = request.form.get('notes')
        sessions_df.at[idx, 'next_actions'] = request.form.get('next_actions')
        sessions_df.at[idx, 'next_session_date'] = request.form.get('next_session_date')
        sessions_df.at[idx, 'fee'] = request.form.get('fee')
        sessions_df.at[idx, 'paid'] = request.form.get('paid')
        sessions_df.at[idx, 'payment_method'] = request.form.get('payment_method')
        sessions_df.at[idx, 'rating'] = request.form.get('rating')
        
        sessions_df.to_csv(SESSIONS_CSV, index=False, encoding='utf-8-sig')
        
        flash('회기 정보가 수정되었습니다.', 'success')
        return redirect(url_for('session_detail', session_id=session_id))
    
    session_info = session_data.fillna('').to_dict('records')[0]
    clients = clients_df.to_dict('records')
    return render_template('session_edit.html', session_data=session_info, clients=clients)

@app.route('/sessions/<session_id>/upload-transcript', methods=['POST'])
@login_required
def upload_transcript(session_id):
    transcript_text = ''
    
    if 'transcript_file' in request.files:
        file = request.files['transcript_file']
        if file and file.filename:
            transcript_text = extract_text_from_file(file)
            if transcript_text is None:
                flash('파일 형식이 지원되지 않거나 파일을 읽을 수 없습니다. TXT, PDF, DOCX, HWP 파일을 사용해주세요.', 'error')
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

@app.route('/sessions/<session_id>/upload-audio', methods=['POST'])
@login_required
def upload_audio(session_id):
    if 'audio_file' not in request.files:
        flash('음성 파일이 선택되지 않았습니다.', 'error')
        return redirect(url_for('session_detail', session_id=session_id))
    
    file = request.files['audio_file']
    
    if not file or not file.filename:
        flash('음성 파일이 선택되지 않았습니다.', 'error')
        return redirect(url_for('session_detail', session_id=session_id))
    
    allowed_extensions = {'.mp3', '.wav', '.m4a', '.ogg', '.webm', '.aac', '.flac'}
    file_ext = os.path.splitext(file.filename)[1].lower()
    
    if file_ext not in allowed_extensions:
        flash(f'지원되지 않는 파일 형식입니다. 허용: {", ".join(allowed_extensions)}', 'error')
        return redirect(url_for('session_detail', session_id=session_id))
    
    filename = f"{session_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}{file_ext}"
    filepath = os.path.join(AUDIO_FOLDER, filename)
    
    try:
        file.save(filepath)
        
        sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
        
        if 'audio_file' not in sessions_df.columns:
            sessions_df['audio_file'] = ''
        
        idx = sessions_df[sessions_df['session_id'] == session_id].index
        
        if len(idx) == 0:
            flash('회기를 찾을 수 없습니다.', 'error')
            return redirect(url_for('sessions_list'))
        
        sessions_df.at[idx[0], 'audio_file'] = filename
        sessions_df.to_csv(SESSIONS_CSV, index=False, encoding='utf-8-sig')
        
        flash('음성 파일이 업로드되었습니다.', 'success')
    except Exception as e:
        flash(f'음성 파일 업로드 오류: {str(e)}', 'error')
    
    return redirect(url_for('session_detail', session_id=session_id))

@app.route('/audio/<filename>')
@login_required
def serve_audio(filename):
    return send_file(os.path.join(AUDIO_FOLDER, filename))

@app.route('/sessions/<session_id>/reanalyze', methods=['POST'])
@login_required
def reanalyze_session(session_id):
    sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
    idx = sessions_df[sessions_df['session_id'] == session_id].index
    
    if len(idx) == 0:
        flash('회기를 찾을 수 없습니다.', 'error')
        return redirect(url_for('sessions_list'))
    
    transcript = sessions_df.at[idx[0], 'transcript']
    
    if not transcript or pd.isna(transcript) or transcript.strip() == '':
        flash('축어록이 없어서 재분석할 수 없습니다.', 'error')
        return redirect(url_for('session_detail', session_id=session_id))
    
    if not openai_client:
        flash('OpenAI API 키가 설정되지 않아 재분석할 수 없습니다.', 'error')
        return redirect(url_for('session_detail', session_id=session_id))
    
    analysis = analyze_transcript(transcript)
    
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
        
        sessions_df.to_csv(SESSIONS_CSV, index=False, encoding='utf-8-sig')
        flash('AI 재분석이 완료되었습니다!', 'success')
    else:
        flash('AI 재분석에 실패했습니다.', 'error')
    
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
    sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    
    if not sessions_df.empty:
        sessions_df = sessions_df.merge(
            clients_df[['client_id', 'name']],
            on='client_id',
            how='left'
        )
        cols = ['session_id', 'client_id', 'name'] + [col for col in sessions_df.columns if col not in ['session_id', 'client_id', 'name']]
        sessions_df = sessions_df[cols]
    
    export_path = f'data/sessions_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    sessions_df.to_csv(export_path, index=False, encoding='utf-8-sig')
    
    return send_file(export_path,
                    as_attachment=True,
                    download_name=f'sessions_{datetime.now().strftime("%Y%m%d")}.csv',
                    mimetype='text/csv')

@app.route('/monthly-revenue')
@login_required
def monthly_revenue():
    year = request.args.get('year', type=int)
    
    sessions_df = pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig')
    
    if sessions_df.empty:
        current_year = datetime.now().year
        return render_template('monthly_revenue.html', 
                             year=current_year, 
                             years=[], 
                             monthly_stats=[],
                             payment_stats={},
                             total_revenue=0,
                             total_paid=0,
                             total_unpaid=0)
    
    sessions_df['date'] = pd.to_datetime(sessions_df['date'], errors='coerce')
    sessions_df = sessions_df.dropna(subset=['date'])
    
    sessions_df['year'] = sessions_df['date'].dt.year
    sessions_df['month'] = sessions_df['date'].dt.month
    sessions_df['fee'] = pd.to_numeric(sessions_df['fee'], errors='coerce').fillna(0)
    
    available_years = sorted(sessions_df['year'].unique(), reverse=True)
    
    if year is None or year not in available_years:
        year = available_years[0] if available_years else datetime.now().year
    
    year_sessions = sessions_df[sessions_df['year'] == year]
    
    monthly_stats = []
    for month in range(1, 13):
        month_sessions = year_sessions[year_sessions['month'] == month]
        
        if month_sessions.empty:
            monthly_stats.append({
                'month': month,
                'total_sessions': 0,
                'total_revenue': 0,
                'paid_revenue': 0,
                'unpaid_revenue': 0,
                'cash': 0,
                'card': 0,
                'voucher': 0,
                'free': 0
            })
        else:
            paid_sessions = month_sessions[month_sessions['paid'] == 'Y']
            unpaid_sessions = month_sessions[month_sessions['paid'] == 'N']
            
            monthly_stats.append({
                'month': month,
                'total_sessions': len(month_sessions),
                'total_revenue': int(month_sessions['fee'].sum()),
                'paid_revenue': int(paid_sessions['fee'].sum()),
                'unpaid_revenue': int(unpaid_sessions['fee'].sum()),
                'cash': int(month_sessions[month_sessions['payment_method'] == '현금']['fee'].sum()),
                'card': int(month_sessions[month_sessions['payment_method'] == '카드']['fee'].sum()),
                'voucher': int(month_sessions[month_sessions['payment_method'] == '바우처']['fee'].sum()),
                'free': int(month_sessions[month_sessions['payment_method'] == '무료']['fee'].sum())
            })
    
    payment_stats = {
        'cash': int(year_sessions[year_sessions['payment_method'] == '현금']['fee'].sum()),
        'card': int(year_sessions[year_sessions['payment_method'] == '카드']['fee'].sum()),
        'voucher': int(year_sessions[year_sessions['payment_method'] == '바우처']['fee'].sum()),
        'free': int(year_sessions[year_sessions['payment_method'] == '무료']['fee'].sum())
    }
    
    total_revenue = int(year_sessions['fee'].sum())
    total_paid = int(year_sessions[year_sessions['paid'] == 'Y']['fee'].sum())
    total_unpaid = int(year_sessions[year_sessions['paid'] == 'N']['fee'].sum())
    
    return render_template('monthly_revenue.html',
                         year=year,
                         years=available_years,
                         monthly_stats=monthly_stats,
                         payment_stats=payment_stats,
                         total_revenue=total_revenue,
                         total_paid=total_paid,
                         total_unpaid=total_unpaid)

@app.route('/download/psychological_test/<client_id>')
@login_required
def download_psychological_test(client_id):
    clients_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    client_data = clients_df[clients_df['client_id'] == client_id]
    
    if client_data.empty:
        flash('내담자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('clients_list'))
    
    client = client_data.fillna('').to_dict('records')[0]
    filename = client.get('psychological_test_file', '')
    
    if not filename:
        flash('심리검사 파일이 없습니다.', 'error')
        return redirect(url_for('client_detail', client_id=client_id))
    
    file_path = os.path.join(PSYCH_TEST_FOLDER, filename)
    
    if not os.path.exists(file_path):
        flash('파일을 찾을 수 없습니다.', 'error')
        return redirect(url_for('client_detail', client_id=client_id))
    
    return send_file(file_path, as_attachment=True, download_name=filename)

if __name__ == '__main__':
    init_csv_files()
    app.run(host='0.0.0.0', port=5000, debug=True)
