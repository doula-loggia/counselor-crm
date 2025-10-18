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
from dotenv import load_dotenv

load_dotenv()

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
SESSION_PARTICIPANTS_CSV = 'data/session_participants.csv'
COUNSELOR_EVENTS_CSV = 'data/counselor_events.csv'
EXAMINEES_CSV = 'data/examinees.csv'
CONSENTS_CSV = 'data/consents.csv'
UPLOAD_FOLDER = 'data/transcripts'
AUDIO_FOLDER = 'data/audio'
PSYCH_TEST_FOLDER = 'data/psychological_tests'
ASSESSMENT_FOLDER = 'data/assessment_reports'
CONSENT_FOLDER = 'data/consent_files'
ALLOWED_PSYCH_TEST_EXTENSIONS = {'.pdf', '.doc', '.docx', '.txt', '.jpg', '.jpeg', '.png'}

EXAMINEE_STATUS_CHOICES = [
    ('evaluating', '평가 진행중'),
    ('completed', '검사 완료'),
    ('counseling_linked', '상담 연계'),
    ('closed', '종결')
]

EXAMINEE_SOURCE_CHOICES = [
    ('new_examinee', '신규 수검자'),
    ('existing_client', '기존 내담자'),
    ('external_referral', '외부 기관')
]

CONSENT_TYPES = [
    ('counseling_agreement', '상담 동의서'),
    ('confidentiality', '비밀보장 및 예외 안내서'),
    ('privacy', '개인정보 수집 및 활용 동의서'),
    ('minor_guardian', '미성년자 상담 보호자 동의서'),
    ('online_recording', '온라인 상담 동의서 (녹음/녹화 포함)')
]

CONSENT_STATUS_CHOICES = [
    ('pending', '대기'),
    ('signed', '서명 완료'),
    ('revoked', '철회')
]

REFERRAL_SOURCE_CHOICES = [
    ('friend', '지인 소개'),
    ('blog', '블로그'),
    ('instagram', '인스타그램'),
    ('naver_ads', '네이버 광고'),
    ('offline', '오프라인 전단'),
    ('other', '기타')
]

openai_client = None
if os.environ.get('OPENAI_API_KEY'):
    openai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

def init_csv_files():
    os.makedirs('data', exist_ok=True)
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(AUDIO_FOLDER, exist_ok=True)
    os.makedirs(PSYCH_TEST_FOLDER, exist_ok=True)
    os.makedirs(ASSESSMENT_FOLDER, exist_ok=True)
    os.makedirs(CONSENT_FOLDER, exist_ok=True)
    
    if not os.path.exists(CLIENTS_CSV):
        clients_df = pd.DataFrame(columns=[
            'client_id', 'name', 'phone', 'email', 'birth_year', 'gender',
            'first_session_date', 'status', 'reengaged_at', 'tags', 'notes', 'medical_history',
            'counseling_history', 'psychological_test_file', 'psychological_test_names',
            'referral_source', 'referral_source_detail'
        ])
        clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
    
    if not os.path.exists(SESSIONS_CSV):
        sessions_df = pd.DataFrame(columns=[
            'session_id', 'client_id', 'date', 'duration_minutes', 'mode',
            'goals', 'interventions', 'tags', 'notes', 'counselor_notes', 'next_actions',
            'next_session_date', 'fee', 'paid', 'payment_method', 'rating', 'transcript',
            'audio_file', 'analysis_summary', 'analysis_stress', 'analysis_intervention',
            'analysis_alternatives', 'analysis_plan', 'analysis_emotions',
            'analysis_distortions', 'analysis_resistance', 'counseling_type'
        ])
        sessions_df.to_csv(SESSIONS_CSV, index=False, encoding='utf-8-sig')
    
    if not os.path.exists(SESSION_PARTICIPANTS_CSV):
        participants_df = pd.DataFrame(columns=['session_id', 'client_id'])
        participants_df.to_csv(SESSION_PARTICIPANTS_CSV, index=False, encoding='utf-8-sig')
    
    if not os.path.exists(COUNSELOR_EVENTS_CSV):
        events_df = pd.DataFrame(columns=[
            'event_id', 'title', 'description', 'start', 'end',
            'all_day', 'category', 'counselor', 'location'
        ])
        events_df.to_csv(COUNSELOR_EVENTS_CSV, index=False, encoding='utf-8-sig')
    
    if not os.path.exists(EXAMINEES_CSV):
        examinees_df = pd.DataFrame(columns=[
            'examinee_id', 'name', 'linked_client_id', 'source', 'birth_year', 'gender',
            'phone', 'email', 'assessment_date', 'assessment_type', 'assessment_tool',
            'assessment_description', 'report_file', 'status', 'notes',
            'created_at', 'updated_at'
        ])
        examinees_df.to_csv(EXAMINEES_CSV, index=False, encoding='utf-8-sig')
    
    if not os.path.exists(CONSENTS_CSV):
        consents_df = pd.DataFrame(columns=[
            'consent_id', 'client_id', 'consent_type', 'status',
            'signed_at', 'expires_at', 'file_path', 'options',
            'notes', 'version', 'created_at', 'updated_at'
        ])
        consents_df.to_csv(CONSENTS_CSV, index=False, encoding='utf-8-sig')

def ensure_client_columns(df):
    required_columns = [
        'psychological_test_file',
        'psychological_test_names',
        'reengaged_at',
        'life_stage',
        'referral_source',
        'referral_source_detail'
    ]
    for column in required_columns:
        if column not in df.columns:
            df[column] = ''
    return df

def ensure_session_columns(df):
    required_columns = [
        'tags', 'notes', 'next_actions', 'next_session_date', 'fee', 'paid',
        'payment_method', 'rating', 'transcript', 'audio_file',
        'analysis_summary', 'analysis_stress', 'analysis_intervention',
        'analysis_alternatives', 'analysis_plan', 'analysis_emotions',
        'analysis_distortions', 'analysis_resistance', 'counselor_notes',
        'counseling_type', 'session_number'
    ]
    for column in required_columns:
        if column not in df.columns:
            df[column] = ''
    df['counseling_type'] = df['counseling_type'].replace('', 'individual').fillna('individual')
    return df

def ensure_examinee_columns(df):
    required_columns = [
        'name', 'linked_client_id', 'source', 'birth_year', 'gender',
        'phone', 'email', 'assessment_date', 'assessment_type',
        'assessment_tool', 'assessment_description', 'report_file',
        'status', 'notes', 'created_at', 'updated_at'
    ]
    if 'examinee_id' not in df.columns:
        df.insert(0, 'examinee_id', '')
    for column in required_columns:
        if column not in df.columns:
            df[column] = ''
    return df

def get_session_participants_dataframe():
    if not os.path.exists(SESSION_PARTICIPANTS_CSV):
        pd.DataFrame(columns=['session_id', 'client_id']).to_csv(
            SESSION_PARTICIPANTS_CSV, index=False, encoding='utf-8-sig'
        )
    participants_df = pd.read_csv(SESSION_PARTICIPANTS_CSV, encoding='utf-8-sig', dtype=str)
    if participants_df.empty:
        return participants_df
    participants_df = participants_df.fillna('')
    if 'session_id' not in participants_df.columns:
        participants_df['session_id'] = ''
    if 'client_id' not in participants_df.columns:
        participants_df['client_id'] = ''
    return participants_df

def save_session_participants_dataframe(df):
    df.to_csv(SESSION_PARTICIPANTS_CSV, index=False, encoding='utf-8-sig')

def set_session_participants(session_id, participant_ids):
    participants_df = get_session_participants_dataframe()
    if not participants_df.empty:
        participants_df = participants_df[participants_df['session_id'] != session_id]
    new_rows = []
    seen = set()
    for client_id in participant_ids:
        client_id = (client_id or '').strip()
        if not client_id or client_id in seen:
            continue
        seen.add(client_id)
        new_rows.append({'session_id': session_id, 'client_id': client_id})
    if new_rows:
        participants_df = pd.concat([participants_df, pd.DataFrame(new_rows)], ignore_index=True)
    save_session_participants_dataframe(participants_df)

def get_participants_by_session(participants_df):
    if participants_df.empty:
        return {}
    grouped = participants_df.groupby('session_id')['client_id'].apply(list)
    return grouped.to_dict()

def annotate_sessions_with_participants(sessions_df, clients_df, participants_df=None):
    sessions_df = sessions_df.copy()
    if participants_df is None:
        participants_df = get_session_participants_dataframe()
    participants_map = get_participants_by_session(participants_df)
    clients_map = clients_df.set_index('client_id')['name'].fillna('').to_dict() if not clients_df.empty else {}
    
    if sessions_df.empty:
        sessions_df['participant_ids'] = []
        sessions_df['participant_names'] = []
        sessions_df['participant_display'] = ''
        sessions_df['primary_client_name'] = ''
        return sessions_df
    
    def resolve_participant_ids(row):
        session_participants = participants_map.get(row['session_id'])
        if session_participants:
            return [pid for pid in session_participants if pid]
        client_id = str(row.get('client_id') or '').strip()
        return [client_id] if client_id else []
    
    sessions_df['participant_ids'] = sessions_df.apply(resolve_participant_ids, axis=1)
    
    def map_ids_to_names(ids):
        names = []
        for cid in ids:
            label = clients_map.get(cid, '').strip()
            names.append(label or cid)
        return names
    
    sessions_df['participant_names'] = sessions_df['participant_ids'].apply(map_ids_to_names)
    sessions_df['participant_display'] = sessions_df['participant_names'].apply(
        lambda names: ', '.join([name for name in names if name]) or '미지정'
    )
    sessions_df['primary_client_name'] = sessions_df['participant_names'].apply(lambda names: names[0] if names else '')
    sessions_df['name'] = sessions_df['primary_client_name']
    if 'session_number' in sessions_df.columns:
        sessions_df['session_number'] = pd.to_numeric(sessions_df['session_number'], errors='coerce')
        if sessions_df['session_number'].isna().any():
            for _, group in sessions_df.groupby('client_id'):
                if group.empty:
                    continue
                ordered = group.sort_values('date')
                if ordered['session_number'].isna().all():
                    sessions_df.loc[ordered.index, 'session_number'] = range(1, len(ordered) + 1)
                else:
                    missing_idx = ordered[ordered['session_number'].isna()].index
                    if not missing_idx.empty:
                        filled = pd.Series(range(1, len(ordered) + 1), index=ordered.index)
                        sessions_df.loc[missing_idx, 'session_number'] = filled.loc[missing_idx]
    return sessions_df

def determine_life_stage(birth_year):
    try:
        birth_year = int(birth_year)
    except (TypeError, ValueError):
        return ''
    current_year = datetime.now().year
    age = current_year - birth_year
    if age < 0:
        return ''
    if age <= 6:
        return 'infant'
    if age <= 12:
        return 'child'
    if age <= 18:
        return 'youth'
    return 'adult'

def get_clients_dataframe():
    raw_df = pd.read_csv(CLIENTS_CSV, encoding='utf-8-sig')
    missing_reengaged_column = 'reengaged_at' not in raw_df.columns
    missing_life_stage_column = 'life_stage' not in raw_df.columns
    df = ensure_client_columns(raw_df)
    if 'status' in df.columns:
        df['status'] = df['status'].replace({
            'active': 'counseling',
            'inactive': 'closed',
            '': 'counseling',
            None: 'counseling'
        })
        df['status'] = df['status'].fillna('counseling')
        df.loc[~df['status'].isin(['counseling', 'closed', 'reengaged']), 'status'] = 'counseling'
    if 'reengaged_at' in df.columns:
        df['reengaged_at'] = df['reengaged_at'].fillna('')
    if 'life_stage' in df.columns:
        df['life_stage'] = df['life_stage'].fillna('')
        updated = False
        for idx, row in df.iterrows():
            if row.get('birth_year'):
                computed_stage = determine_life_stage(row['birth_year'])
                if computed_stage and row.get('life_stage') != computed_stage:
                    df.at[idx, 'life_stage'] = computed_stage
                    updated = True
        if updated:
            df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
            return df
    if missing_reengaged_column or missing_life_stage_column:
        df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
    return df

def get_clients_records():
    clients_df = get_clients_dataframe()
    clients_list = clients_df.fillna('').to_dict('records')
    return clients_df, clients_list

def get_examinees_dataframe():
    return ensure_examinee_columns(pd.read_csv(EXAMINEES_CSV, encoding='utf-8-sig'))

def get_client_consents(client_id):
    consents_df = get_consents_dataframe()
    if consents_df.empty:
        return [], consents_df
    client_consents = consents_df[consents_df['client_id'] == client_id].fillna('')
    return client_consents.to_dict('records'), consents_df

def ensure_counselor_event_columns(df):
    required_columns = [
        'event_id', 'title', 'description', 'start', 'end',
        'all_day', 'category', 'counselor', 'location'
    ]
    for column in required_columns:
        if column not in df.columns:
            df[column] = ''
    return df

def get_counselor_events_dataframe():
    if not os.path.exists(COUNSELOR_EVENTS_CSV):
        pd.DataFrame(columns=[
            'event_id', 'title', 'description', 'start', 'end',
            'all_day', 'category', 'counselor', 'location'
        ]).to_csv(COUNSELOR_EVENTS_CSV, index=False, encoding='utf-8-sig')
    raw_df = pd.read_csv(COUNSELOR_EVENTS_CSV, encoding='utf-8-sig')
    df = ensure_counselor_event_columns(raw_df)
    if not raw_df.equals(df):
        df.to_csv(COUNSELOR_EVENTS_CSV, index=False, encoding='utf-8-sig')
    return df

def ensure_consent_columns(df):
    required_columns = [
        'client_id', 'consent_type', 'status',
        'signed_at', 'expires_at', 'file_path', 'options',
        'notes', 'version', 'created_at', 'updated_at'
    ]
    if 'consent_id' not in df.columns:
        df.insert(0, 'consent_id', '')
    for column in required_columns:
        if column not in df.columns:
            df[column] = ''
    return df

def get_consents_dataframe():
    raw_df = pd.read_csv(CONSENTS_CSV, encoding='utf-8-sig')
    missing_cols = ensure_consent_columns(raw_df)
    if not raw_df.equals(missing_cols):
        missing_cols.to_csv(CONSENTS_CSV, index=False, encoding='utf-8-sig')
    return missing_cols

def ensure_required_consents_for_client(client_id, default_status='pending'):
    consents_df = get_consents_dataframe()
    created_count = 0
    for consent_type, _ in CONSENT_TYPES:
        if consent_type not in ['counseling_agreement', 'confidentiality', 'privacy']:
            continue
        exists = False
        if not consents_df.empty:
            exists = ((consents_df['client_id'] == client_id) &
                      (consents_df['consent_type'] == consent_type)).any()
        if not exists:
            consent_id = generate_consent_id()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            new_row = {
                'consent_id': consent_id,
                'client_id': client_id,
                'consent_type': consent_type,
                'status': default_status,
                'signed_at': '' if default_status != 'signed' else timestamp.split(' ')[0],
                'expires_at': '',
                'file_path': '',
                'options': '',
                'notes': '',
                'version': 'v1',
                'created_at': timestamp,
                'updated_at': timestamp
            }
            consents_df = pd.concat([consents_df, pd.DataFrame([new_row])], ignore_index=True)
            created_count += 1
    if created_count:
        consents_df.to_csv(CONSENTS_CSV, index=False, encoding='utf-8-sig')
    return created_count

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def _generate_short_id(prefix: str) -> str:
    base = datetime.now().strftime('%y%m%d')
    random_suffix = secrets.token_hex(2).upper()
    return f"{prefix}{base}-{random_suffix}"

def generate_client_id():
    return _generate_short_id('C')

def generate_session_id():
    return _generate_short_id('S')

def generate_examinee_id():
    return _generate_short_id('E')

def generate_consent_id():
    return _generate_short_id('CN')

def generate_event_id():
    return _generate_short_id('EV')

def parse_bool(value):
    return str(value).strip().lower() in ['true', '1', 'y', 'yes', 'on', 'checked']

def to_datetime_local(value):
    if value is None or str(value).strip() == '':
        return ''
    try:
        dt = pd.to_datetime(value)
    except Exception:
        return ''
    if pd.isna(dt):
        return ''
    return dt.strftime('%Y-%m-%dT%H:%M')

DEFAULT_SESSION_DURATION_MINUTES = 50
DEFAULT_EVENT_DURATION_MINUTES = 60

def parse_datetime(value):
    if value is None or str(value).strip() == '':
        return None
    try:
        dt = pd.to_datetime(value)
    except Exception:
        return None
    if pd.isna(dt):
        return None
    return dt

def prepare_event_range(start_dt, end_dt, all_day=False, default_minutes=DEFAULT_EVENT_DURATION_MINUTES):
    if start_dt is None or pd.isna(start_dt):
        return None, None
    if all_day:
        start_norm = start_dt.normalize()
        if end_dt is None or pd.isna(end_dt):
            end_norm = start_norm + pd.Timedelta(days=1)
        else:
            end_norm = end_dt.normalize() + pd.Timedelta(days=1)
        return start_norm, end_norm
    if end_dt is None or pd.isna(end_dt) or end_dt <= start_dt:
        end_dt = start_dt + pd.Timedelta(minutes=default_minutes)
    return start_dt, end_dt

def ranges_overlap(start_a, end_a, start_b, end_b):
    return start_a < end_b and start_b < end_a

def counselors_conflict(existing_counselor, new_counselor):
    existing = (existing_counselor or '').strip()
    new = (new_counselor or '').strip()
    if not existing or not new:
        return True
    return existing.lower() == new.lower()

def find_overlapping_sessions(new_start, new_end, counselor_name=None):
    messages = []
    if new_start is None or new_end is None:
        return messages
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
    if sessions_df.empty:
        return messages
    clients_df = get_clients_dataframe()
    participants_df = get_session_participants_dataframe()
    sessions_df = annotate_sessions_with_participants(sessions_df, clients_df, participants_df)
    sessions_df['date_dt'] = pd.to_datetime(sessions_df['date'], errors='coerce')
    for _, row in sessions_df.iterrows():
        s_start = row.get('date_dt')
        if pd.isna(s_start):
            continue
        try:
            duration = int(row.get('duration_minutes') or 0)
        except (TypeError, ValueError):
            duration = DEFAULT_SESSION_DURATION_MINUTES
        if duration <= 0:
            duration = DEFAULT_SESSION_DURATION_MINUTES
        s_end = s_start + pd.Timedelta(minutes=duration)
        if ranges_overlap(new_start, new_end, s_start, s_end):
            participants = row.get('participant_display') or row.get('name') or ''
            when_label = s_start.strftime('%Y-%m-%d %H:%M')
            messages.append(f"{when_label}에 진행되는 상담 회기({participants})와 시간이 겹칩니다.")
    return messages

def find_overlapping_personal_events(new_start, new_end, counselor_name=None, exclude_event_id=None):
    messages = []
    if new_start is None or new_end is None:
        return messages
    events_df = get_counselor_events_dataframe()
    if events_df.empty:
        return messages
    for _, row in events_df.iterrows():
        if exclude_event_id and str(row.get('event_id')) == str(exclude_event_id):
            continue
        if not counselors_conflict(row.get('counselor'), counselor_name):
            continue
        existing_start = parse_datetime(row.get('start'))
        existing_end = parse_datetime(row.get('end'))
        event_start, event_end = prepare_event_range(existing_start, existing_end, parse_bool(row.get('all_day')))
        if event_start is None or event_end is None:
            continue
        if ranges_overlap(new_start, new_end, event_start, event_end):
            title = row.get('title') or '상담자 일정'
            if parse_bool(row.get('all_day')):
                when_label = event_start.strftime('%Y-%m-%d (종일)')
            else:
                when_label = event_start.strftime('%Y-%m-%d %H:%M')
            messages.append(f"{when_label}에 등록된 개인 일정 '{title}'과 시간이 겹칩니다.")
    return messages

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
    clients_df = get_clients_dataframe()
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
    participants_df = get_session_participants_dataframe()
    examinees_df = get_examinees_dataframe()
    consents_df = get_consents_dataframe()
    
    total_clients = len(clients_df)
    counseling_clients = len(clients_df[clients_df['status'].isin(['counseling', 'reengaged'])])
    reengaged_clients = len(clients_df[clients_df['status'] == 'reengaged'])
    total_sessions = len(sessions_df)
    total_examinees = len(examinees_df)
    linked_examinees = len(examinees_df[examinees_df['linked_client_id'].astype(str).str.strip() != '']) if not examinees_df.empty else 0
    
    upcoming_sessions = []
    if not sessions_df.empty:
        sessions_df = annotate_sessions_with_participants(sessions_df, clients_df, participants_df)
        sessions_df = sessions_df.fillna('')
        sessions_df['date_dt'] = pd.to_datetime(sessions_df['date'], errors='coerce')
        today = pd.Timestamp.now().normalize()
        future_sessions = sessions_df[sessions_df['date_dt'] >= today].sort_values('date_dt')
        if future_sessions.empty:
            future_sessions = sessions_df.sort_values('date_dt', ascending=False)
        for _, row in future_sessions.head(5).iterrows():
            date_label = row['date_dt'].strftime('%Y-%m-%d') if pd.notna(row['date_dt']) else row.get('date', '')
            upcoming_sessions.append({
                'session_id': row.get('session_id'),
                'date_label': date_label,
                'participants': row.get('participant_display') or row.get('name') or '',
                'mode': row.get('mode') or '',
                'url': url_for('session_detail', session_id=row.get('session_id'))
            })

    required_consents = ['counseling_agreement', 'confidentiality', 'privacy']
    consent_label_map = dict(CONSENT_TYPES)
    missing_consents = []

    if not clients_df.empty:
        for _, client_row in clients_df.fillna('').iterrows():
            client_id = client_row['client_id']
            client_name = client_row['name']
            if consents_df.empty:
                missing_types = required_consents.copy()
            else:
                client_consents = consents_df[(consents_df['client_id'] == client_id) & (consents_df['consent_type'].isin(required_consents))]
                signed_types = set(client_consents[client_consents['status'] == 'signed']['consent_type'])
                missing_types = [c for c in required_consents if c not in signed_types]
            if missing_types:
                missing_consents.append({
                    'client_id': client_id,
                    'client_name': client_name,
                    'missing_labels': [consent_label_map.get(c, c) for c in missing_types]
                })
    missing_consents_count = len(missing_consents)
    
    today = datetime.now()
    weekday_labels = ['월', '화', '수', '목', '금', '토', '일']
    today_label = today.strftime('%Y.%m.%d')
    today_weekday_idx = today.weekday()
    
    return render_template('home.html', 
                         total_clients=total_clients,
                         counseling_clients=counseling_clients,
                         reengaged_clients=reengaged_clients,
                         total_sessions=total_sessions,
                         total_examinees=total_examinees,
                         linked_examinees=linked_examinees,
                         missing_consents=missing_consents,
                         missing_consents_count=missing_consents_count,
                         upcoming_sessions=upcoming_sessions,
                         today_label=today_label,
                         weekday_labels=weekday_labels,
                         today_weekday_idx=today_weekday_idx)

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
    clients_df = get_clients_dataframe()
    referral_source_map = dict(REFERRAL_SOURCE_CHOICES)
    search_query = request.args.get('search', '').strip()
    status_filter = request.args.get('status', '').strip()
    
    status_filter_mapped = {
        'active': 'counseling',
        'inactive': 'closed',
        'reengaged': 'reengaged'
    }.get(status_filter, status_filter)
    
    if status_filter_mapped:
        clients_df = clients_df[clients_df['status'] == status_filter_mapped]
    
    if status_filter:
        status_filter = status_filter_mapped

    if 'referral_source' in clients_df.columns:
        clients_df['referral_source_label'] = clients_df['referral_source'].map(referral_source_map).fillna('')
    else:
        clients_df['referral_source_label'] = ''

    if search_query:
        mask = (
            clients_df['name'].str.contains(search_query, case=False, na=False) |
            clients_df['tags'].str.contains(search_query, case=False, na=False) |
            clients_df['referral_source_label'].str.contains(search_query, case=False, na=False) |
            clients_df['referral_source_detail'].str.contains(search_query, case=False, na=False)
        )
        clients_df = clients_df[mask]
    
    clients_df = clients_df.fillna('')
    status_order = {'counseling': 0, 'reengaged': 1, 'closed': 2}
    if 'status' in clients_df.columns:
        clients_df['status_sort'] = clients_df['status'].map(status_order).fillna(3)
        clients_df = clients_df.sort_values(['status_sort', 'name'])
    else:
        clients_df['status_sort'] = 3
    clients = clients_df.drop(columns=['status_sort'], errors='ignore').to_dict('records')
    
    return render_template(
        'clients_list.html',
        clients=clients,
        search_query=search_query,
        status_filter=status_filter,
        referral_source_labels=referral_source_map
    )

@app.route('/clients/new', methods=['GET', 'POST'])
@login_required
def client_new():
    clients_df = get_clients_dataframe()

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
        
        birth_year = request.form.get('birth_year')
        life_stage = determine_life_stage(birth_year)
        referral_source = (request.form.get('referral_source') or '').strip()
        referral_source_detail = (request.form.get('referral_source_detail') or '').strip()
        if referral_source != 'other':
            referral_source_detail = ''

        new_client = {
            'client_id': client_id,
            'name': request.form.get('name'),
            'phone': request.form.get('phone'),
            'email': request.form.get('email'),
            'birth_year': birth_year,
            'gender': request.form.get('gender'),
            'first_session_date': request.form.get('first_session_date'),
            'status': 'counseling',
            'reengaged_at': '',
            'life_stage': life_stage or '',
            'tags': request.form.get('tags'),
            'notes': request.form.get('notes'),
            'medical_history': request.form.get('medical_history'),
            'counseling_history': request.form.get('counseling_history'),
            'psychological_test_file': psychological_test_filename,
            'psychological_test_names': request.form.get('psychological_test_names', ''),
            'referral_source': referral_source,
            'referral_source_detail': referral_source_detail
        }

        clients_df = pd.concat([clients_df, pd.DataFrame([new_client])], ignore_index=True)
        clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
        ensure_required_consents_for_client(client_id)

        flash('새 내담자가 추가되었습니다.', 'success')
        return redirect(url_for('clients_list'))
    
    all_tags = set()
    for tags_str in clients_df['tags'].dropna():
        if tags_str:
            tags = [tag.strip() for tag in str(tags_str).split(',')]
            all_tags.update(tags)
    
    existing_tags = sorted(list(all_tags))
    return render_template(
        'client_form.html',
        existing_tags=existing_tags,
        referral_source_choices=REFERRAL_SOURCE_CHOICES
    )

@app.route('/clients/<client_id>')
@login_required
def client_detail(client_id):
    clients_df = get_clients_dataframe()
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
    participants_df = get_session_participants_dataframe()
    examinees_df = get_examinees_dataframe()
    client_consents, _ = get_client_consents(client_id)
    
    client_data = clients_df[clients_df['client_id'] == client_id]
    if client_data.empty:
        flash('내담자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('clients_list'))
    
    client_info = client_data.fillna('').to_dict('records')[0]
    
    participant_session_ids = participants_df[participants_df['client_id'] == client_id]['session_id'].tolist()
    client_sessions = sessions_df[
        (sessions_df['client_id'] == client_id) | (sessions_df['session_id'].isin(participant_session_ids))
    ]
    if not client_sessions.empty:
        client_sessions = annotate_sessions_with_participants(client_sessions, clients_df, participants_df)
        client_sessions = client_sessions.sort_values('date', ascending=False)
    
    client_sessions = client_sessions.fillna('')
    sessions = client_sessions.to_dict('records')
    
    linked_examinees = examinees_df[examinees_df['linked_client_id'] == client_id]
    if not linked_examinees.empty:
        linked_examinees = linked_examinees.sort_values('assessment_date', ascending=False)
    examinees = linked_examinees.fillna('').to_dict('records')

    consents_list = client_consents
    consents_list.sort(key=lambda c: (c.get('signed_at') or c.get('updated_at') or ''), reverse=True)
    
    return render_template(
        'client_detail.html',
        client=client_info,
        sessions=sessions,
        examinees=examinees,
        consents=consents_list,
        consent_types=dict(CONSENT_TYPES),
        consent_statuses=dict(CONSENT_STATUS_CHOICES),
        examinee_status_labels=dict(EXAMINEE_STATUS_CHOICES),
        examinee_source_labels=dict(EXAMINEE_SOURCE_CHOICES),
        referral_source_labels=dict(REFERRAL_SOURCE_CHOICES)
    )

@app.route('/clients/<client_id>/edit', methods=['GET', 'POST'])
@login_required
def client_edit(client_id):
    clients_df = get_clients_dataframe()
    
    client_data = clients_df[clients_df['client_id'] == client_id]
    if client_data.empty:
        flash('내담자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('clients_list'))
    
    if request.method == 'POST':
        idx = clients_df[clients_df['client_id'] == client_id].index[0]
        
        clients_df.at[idx, 'name'] = request.form.get('name')
        clients_df.at[idx, 'phone'] = request.form.get('phone')
        clients_df.at[idx, 'email'] = request.form.get('email')
        birth_year = request.form.get('birth_year')
        clients_df.at[idx, 'birth_year'] = birth_year
        clients_df.at[idx, 'gender'] = request.form.get('gender')
        clients_df.at[idx, 'first_session_date'] = request.form.get('first_session_date')
        new_status = request.form.get('status')
        clients_df.at[idx, 'status'] = new_status
        clients_df.at[idx, 'tags'] = request.form.get('tags')
        clients_df.at[idx, 'notes'] = request.form.get('notes')
        clients_df.at[idx, 'medical_history'] = request.form.get('medical_history')
        clients_df.at[idx, 'counseling_history'] = request.form.get('counseling_history')
        clients_df.at[idx, 'psychological_test_names'] = request.form.get('psychological_test_names', '')
        life_stage = determine_life_stage(birth_year) if birth_year else clients_df.at[idx, 'life_stage']
        clients_df.at[idx, 'life_stage'] = life_stage or ''
        referral_source = (request.form.get('referral_source') or '').strip()
        referral_source_detail = (request.form.get('referral_source_detail') or '').strip()
        if referral_source != 'other':
            referral_source_detail = ''
        clients_df.at[idx, 'referral_source'] = referral_source
        clients_df.at[idx, 'referral_source_detail'] = referral_source_detail

        reengaged_at_form = (request.form.get('reengaged_at') or '').strip()
        current_reengaged = str(clients_df.at[idx, 'reengaged_at']).strip() if 'reengaged_at' in clients_df.columns else ''
        if new_status == 'reengaged':
            if not reengaged_at_form:
                reengaged_at_form = datetime.now().strftime('%Y-%m-%d')
            clients_df.at[idx, 'reengaged_at'] = reengaged_at_form
        elif new_status == 'counseling':
            clients_df.at[idx, 'reengaged_at'] = ''
        else:  # closed
            if reengaged_at_form:
                clients_df.at[idx, 'reengaged_at'] = reengaged_at_form
            elif current_reengaged:
                clients_df.at[idx, 'reengaged_at'] = current_reengaged

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
    return render_template(
        'client_edit.html',
        client=client,
        existing_tags=existing_tags,
        referral_source_choices=REFERRAL_SOURCE_CHOICES
    )

@app.route('/sessions')
@login_required
def sessions_list():
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
    clients_df = get_clients_dataframe()
    sessions_df = annotate_sessions_with_participants(sessions_df, clients_df)
    
    if not sessions_df.empty:
        sessions_df = sessions_df.sort_values('date', ascending=False)
    
    sessions_df = sessions_df.fillna('')
    sessions = sessions_df.to_dict('records')
    
    return render_template('sessions_list.html', sessions=sessions)

@app.route('/sessions/new', methods=['GET', 'POST'])
@login_required
def session_new():
    clients_df = get_clients_dataframe()
    preselected_client_id = request.args.get('client_id', '').strip()
    
    if request.method == 'POST':
        sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
        counseling_type = request.form.get('counseling_type', 'individual')
        counseling_type = counseling_type if counseling_type in ['group', 'individual'] else 'individual'
        primary_client_id = (request.form.get('client_id') or '').strip()
        participant_ids = [pid.strip() for pid in request.form.getlist('participant_ids') if pid.strip()]
        
        if counseling_type == 'group':
            if not participant_ids and not primary_client_id:
                flash('집단 상담의 참여 내담자를 한 명 이상 선택해주세요.', 'error')
                return redirect(url_for('session_new'))
            if primary_client_id and primary_client_id not in participant_ids:
                participant_ids.insert(0, primary_client_id)
            if not primary_client_id and participant_ids:
                primary_client_id = participant_ids[0]
        else:
            if not primary_client_id:
                flash('내담자를 선택해주세요.', 'error')
            return redirect(url_for('session_new'))
        participant_ids = [primary_client_id]

        seen_participants = set()
        cleaned_participants = []
        largest_existing_number = sessions_df[sessions_df['client_id'] == primary_client_id]['session_number'].astype(float).dropna().max() if 'session_number' in sessions_df.columns else None
        for cid in participant_ids:
            if cid and cid not in seen_participants:
                seen_participants.add(cid)
                cleaned_participants.append(cid)
        participant_ids = cleaned_participants

        if 'session_number' in sessions_df.columns:
            client_numbers = pd.to_numeric(sessions_df[sessions_df['client_id'] == primary_client_id]['session_number'], errors='coerce')
            largest_existing_number = client_numbers.dropna().max()
        else:
            largest_existing_number = None

        new_session = {
            'session_id': generate_session_id(),
            'client_id': primary_client_id,
            'date': request.form.get('date'),
            'duration_minutes': request.form.get('duration_minutes'),
            'mode': request.form.get('mode'),
            'goals': request.form.get('goals'),
            'interventions': request.form.get('interventions'),
            'tags': request.form.get('tags'),
            'notes': request.form.get('notes'),
            'counselor_notes': request.form.get('counselor_notes'),
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
            'analysis_resistance': '',
            'counseling_type': counseling_type,
            'session_number': int(largest_existing_number or 0) + 1
        }
        
        sessions_df = pd.concat([sessions_df, pd.DataFrame([new_session])], ignore_index=True)
        sessions_df.to_csv(SESSIONS_CSV, index=False, encoding='utf-8-sig')
        set_session_participants(new_session['session_id'], participant_ids or ([primary_client_id] if primary_client_id else []))
        
        clients_updated = False
        for participant_id in participant_ids or ([primary_client_id] if primary_client_id else []):
            if not participant_id:
                continue
            client_idx = clients_df[clients_df['client_id'] == participant_id].index
            if len(client_idx) == 0:
                continue
            idx_client = client_idx[0]
            current_status = clients_df.at[idx_client, 'status']
            reengaged_value = str(clients_df.at[idx_client, 'reengaged_at']).strip() if 'reengaged_at' in clients_df.columns else ''
            update_needed = False
            if current_status == 'closed':
                clients_df.at[idx_client, 'status'] = 'reengaged'
                if not reengaged_value:
                    clients_df.at[idx_client, 'reengaged_at'] = new_session['date'] or datetime.now().strftime('%Y-%m-%d')
                update_needed = True
            elif current_status == 'reengaged' and not reengaged_value:
                clients_df.at[idx_client, 'reengaged_at'] = new_session['date'] or datetime.now().strftime('%Y-%m-%d')
                update_needed = True
            if update_needed:
                clients_updated = True
        if clients_updated:
            clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')

        flash('새 회기가 추가되었습니다.', 'success')
        return redirect(url_for('sessions_list'))
    
    clients = clients_df.to_dict('records')
    return render_template('session_form.html', clients=clients, preselected_client_id=preselected_client_id)

@app.route('/sessions/<session_id>')
@login_required
def session_detail(session_id):
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
    clients_df = get_clients_dataframe()
    participants_df = get_session_participants_dataframe()
    
    session_data = sessions_df[sessions_df['session_id'] == session_id]
    if session_data.empty:
        flash('회기를 찾을 수 없습니다.', 'error')
        return redirect(url_for('sessions_list'))
    
    enriched_session = annotate_sessions_with_participants(session_data, clients_df, participants_df)
    session_info = enriched_session.fillna('').to_dict('records')[0]
    participant_ids = session_info.get('participant_ids') or []
    if not participant_ids:
        fallback_client_id = (session_info.get('client_id') or '').strip()
        participant_ids = [fallback_client_id] if fallback_client_id else []
        session_info['participant_ids'] = participant_ids
        clients_map = clients_df.set_index('client_id')['name'].fillna('').to_dict() if not clients_df.empty else {}
        session_info['participant_names'] = [clients_map.get(fallback_client_id, fallback_client_id)]
        session_info['participant_display'] = ', '.join(session_info['participant_names'])
        session_info['primary_client_name'] = session_info['participant_names'][0] if session_info['participant_names'] else ''
        session_info['name'] = session_info['primary_client_name']
    session_info['participant_count'] = len(session_info.get('participant_ids') or [])
    session_info['counseling_type'] = session_info.get('counseling_type') or 'individual'
    session_info['counseling_type_label'] = '집단 상담' if session_info['counseling_type'] == 'group' else '개별 상담'
    if not session_info.get('name'):
        session_info['name'] = session_info.get('primary_client_name', '')
    if not session_info.get('participant_display'):
        session_info['participant_display'] = session_info['name']
    session_info['participant_names'] = session_info.get('participant_names') or [session_info['name']]
    
    return render_template('session_detail.html', session_data=session_info)

@app.route('/sessions/<session_id>/edit', methods=['GET', 'POST'])
@login_required
def session_edit(session_id):
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
    clients_df = get_clients_dataframe()
    participants_df = get_session_participants_dataframe()
    
    session_data = sessions_df[sessions_df['session_id'] == session_id]
    if session_data.empty:
        flash('회기를 찾을 수 없습니다.', 'error')
        return redirect(url_for('sessions_list'))
    
    if request.method == 'POST':
        idx = sessions_df[sessions_df['session_id'] == session_id].index[0]
        counseling_type = request.form.get('counseling_type', 'individual')
        counseling_type = counseling_type if counseling_type in ['group', 'individual'] else 'individual'
        primary_client_id = (request.form.get('client_id') or '').strip()
        participant_ids = [pid.strip() for pid in request.form.getlist('participant_ids') if pid.strip()]
        
        if counseling_type == 'group':
            if not participant_ids and not primary_client_id:
                flash('집단 상담의 참여 내담자를 한 명 이상 선택해주세요.', 'error')
                return redirect(url_for('session_edit', session_id=session_id))
            if primary_client_id and primary_client_id not in participant_ids:
                participant_ids.insert(0, primary_client_id)
            if not primary_client_id and participant_ids:
                primary_client_id = participant_ids[0]
        else:
            if not primary_client_id:
                flash('내담자를 선택해주세요.', 'error')
                return redirect(url_for('session_edit', session_id=session_id))
            participant_ids = [primary_client_id]
        
        seen_participants = set()
        cleaned_participants = []
        for cid in participant_ids:
            if cid and cid not in seen_participants:
                seen_participants.add(cid)
                cleaned_participants.append(cid)
        participant_ids = cleaned_participants
        
        sessions_df.at[idx, 'client_id'] = primary_client_id
        sessions_df.at[idx, 'date'] = request.form.get('date')
        sessions_df.at[idx, 'duration_minutes'] = request.form.get('duration_minutes')
        sessions_df.at[idx, 'mode'] = request.form.get('mode')
        sessions_df.at[idx, 'goals'] = request.form.get('goals')
        sessions_df.at[idx, 'interventions'] = request.form.get('interventions')
        sessions_df.at[idx, 'tags'] = request.form.get('tags')
        sessions_df.at[idx, 'notes'] = request.form.get('notes')
        sessions_df.at[idx, 'counselor_notes'] = request.form.get('counselor_notes')
        sessions_df.at[idx, 'next_actions'] = request.form.get('next_actions')
        sessions_df.at[idx, 'next_session_date'] = request.form.get('next_session_date')
        sessions_df.at[idx, 'fee'] = request.form.get('fee')
        sessions_df.at[idx, 'paid'] = request.form.get('paid')
        sessions_df.at[idx, 'payment_method'] = request.form.get('payment_method')
        sessions_df.at[idx, 'rating'] = request.form.get('rating')
        sessions_df.at[idx, 'counseling_type'] = counseling_type
        if sessions_df.at[idx, 'client_id']:
            client_sessions = sessions_df[sessions_df['client_id'] == sessions_df.at[idx, 'client_id']].sort_values('date')
            client_sessions = client_sessions.reset_index(drop=True)
            client_sessions['session_number'] = client_sessions.index + 1
            for _, row in client_sessions.iterrows():
                sessions_df.loc[sessions_df['session_id'] == row['session_id'], 'session_number'] = int(row['session_number'])
        
        sessions_df.to_csv(SESSIONS_CSV, index=False, encoding='utf-8-sig')
        set_session_participants(session_id, participant_ids or ([primary_client_id] if primary_client_id else []))
        
        clients_updated = False
        for participant_id in participant_ids or ([primary_client_id] if primary_client_id else []):
            if not participant_id:
                continue
            client_idx = clients_df[clients_df['client_id'] == participant_id].index
            if len(client_idx) == 0:
                continue
            idx_client = client_idx[0]
            current_status = clients_df.at[idx_client, 'status']
            reengaged_value = str(clients_df.at[idx_client, 'reengaged_at']).strip() if 'reengaged_at' in clients_df.columns else ''
            update_needed = False
            if current_status == 'closed':
                clients_df.at[idx_client, 'status'] = 'reengaged'
                if not reengaged_value:
                    session_date = request.form.get('date') or datetime.now().strftime('%Y-%m-%d')
                    clients_df.at[idx_client, 'reengaged_at'] = session_date
                update_needed = True
            elif current_status == 'reengaged' and not reengaged_value:
                session_date = request.form.get('date') or datetime.now().strftime('%Y-%m-%d')
                clients_df.at[idx_client, 'reengaged_at'] = session_date
                update_needed = True
            if update_needed:
                clients_updated = True
        if clients_updated:
            clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
        
        flash('회기 정보가 수정되었습니다.', 'success')
        return redirect(url_for('session_detail', session_id=session_id))
    
    enriched_session = annotate_sessions_with_participants(session_data, clients_df, participants_df)
    session_info = enriched_session.fillna('').to_dict('records')[0]
    if not session_info.get('participant_ids'):
        fallback_client_id = (session_info.get('client_id') or '').strip()
        session_info['participant_ids'] = [fallback_client_id] if fallback_client_id else []
    session_info['counseling_type'] = session_info.get('counseling_type') or 'individual'
    clients = clients_df.to_dict('records')
    return render_template('session_edit.html', session_data=session_info, clients=clients)

@app.route('/examinees')
@login_required
def examinees_list():
    examinees_df = get_examinees_dataframe()
    clients_df = get_clients_dataframe()
    
    search_query = request.args.get('search', '').strip()
    status_filter = request.args.get('status', '').strip()
    source_filter = request.args.get('source', '').strip()
    
    if search_query:
        mask = (
            examinees_df['name'].str.contains(search_query, case=False, na=False) |
            examinees_df['assessment_type'].str.contains(search_query, case=False, na=False) |
            examinees_df['assessment_tool'].str.contains(search_query, case=False, na=False)
        )
        examinees_df = examinees_df[mask]
    
    if status_filter:
        examinees_df = examinees_df[examinees_df['status'] == status_filter]
    
    if source_filter:
        examinees_df = examinees_df[examinees_df['source'] == source_filter]
    
    if not examinees_df.empty:
        examinees_df['assessment_sort'] = pd.to_datetime(examinees_df['assessment_date'], errors='coerce')
        examinees_df['updated_sort'] = pd.to_datetime(examinees_df['updated_at'], errors='coerce')
        examinees_df = examinees_df.sort_values(
            ['assessment_sort', 'updated_sort'],
            ascending=[False, False]
        ).drop(columns=['assessment_sort', 'updated_sort'])
    
    clients_map = clients_df.set_index('client_id')['name'].fillna('').to_dict() if not clients_df.empty else {}
    examinees_df = examinees_df.fillna('')
    examinees = []
    for _, row in examinees_df.iterrows():
        row_dict = row.to_dict()
        row_dict['linked_client_name'] = clients_map.get(row_dict.get('linked_client_id', ''), '')
        examinees.append(row_dict)
    
    return render_template(
        'examinees_list.html',
        examinees=examinees,
        search_query=search_query,
        status_filter=status_filter,
        source_filter=source_filter,
        status_choices=EXAMINEE_STATUS_CHOICES,
        source_choices=EXAMINEE_SOURCE_CHOICES,
        status_labels=dict(EXAMINEE_STATUS_CHOICES),
        source_labels=dict(EXAMINEE_SOURCE_CHOICES)
    )

@app.route('/examinees/new', methods=['GET', 'POST'])
@login_required
def examinee_new():
    clients_df, clients = get_clients_records()
    preselected_client_id = request.args.get('client_id', '').strip()
    default_source = 'existing_client' if preselected_client_id else 'new_examinee'
    
    if request.method == 'POST':
        examinees_df = get_examinees_dataframe()
        examinee_id = generate_examinee_id()
        linked_client_id = request.form.get('linked_client_id', '').strip()
        source = request.form.get('source', '').strip() or default_source
        status = request.form.get('status', '').strip()
        name = request.form.get('name', '').strip()
        birth_year = request.form.get('birth_year', '').strip()
        gender = request.form.get('gender', '').strip()
        phone = request.form.get('phone', '').strip()
        email = request.form.get('email', '').strip()
        assessment_date = request.form.get('assessment_date', '').strip()
        assessment_type = request.form.get('assessment_type', '').strip()
        assessment_tool = request.form.get('assessment_tool', '').strip()
        assessment_description = request.form.get('assessment_description', '').strip()
        notes = request.form.get('notes', '').strip()
        
        client_info = None
        if linked_client_id:
            client_row = clients_df[clients_df['client_id'] == linked_client_id]
            if client_row.empty:
                flash('선택한 내담자를 찾을 수 없습니다.', 'error')
                return redirect(url_for('examinee_new'))
            client_info = client_row.fillna('').to_dict('records')[0]
        
        if client_info:
            name = name or client_info.get('name', '')
            birth_year = birth_year or str(client_info.get('birth_year', '') or '')
            gender = gender or client_info.get('gender', '')
            phone = phone or client_info.get('phone', '')
            email = email or client_info.get('email', '')
        
        if not name:
            flash('수검자 이름을 입력해주세요.', 'error')
            return redirect(url_for('examinee_new'))
        
        if not source:
            source = 'existing_client' if linked_client_id else 'new_examinee'
        if not status:
            status = 'evaluating'
        
        report_file = request.files.get('report_file')
        report_filename = ''
        if report_file and report_file.filename:
            file_ext = os.path.splitext(report_file.filename)[1].lower()
            if file_ext not in ALLOWED_PSYCH_TEST_EXTENSIONS:
                flash('허용되지 않는 파일 형식입니다. PDF, DOC, DOCX, TXT, JPG, PNG 파일만 업로드 가능합니다.', 'error')
                return redirect(url_for('examinee_new'))
            safe_name = secure_filename(report_file.filename)
            report_filename = f"{examinee_id}_{safe_name}"
            report_path = os.path.join(ASSESSMENT_FOLDER, report_filename)
            report_file.save(report_path)
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        new_examinee = {
            'examinee_id': examinee_id,
            'name': name,
            'linked_client_id': linked_client_id,
            'source': source,
            'birth_year': birth_year,
            'gender': gender,
            'phone': phone,
            'email': email,
            'assessment_date': assessment_date,
            'assessment_type': assessment_type,
            'assessment_tool': assessment_tool,
            'assessment_description': assessment_description,
            'report_file': report_filename,
            'status': status,
            'notes': notes,
            'created_at': timestamp,
            'updated_at': timestamp
        }
        
        examinees_df = pd.concat([examinees_df, pd.DataFrame([new_examinee])], ignore_index=True)
        examinees_df.to_csv(EXAMINEES_CSV, index=False, encoding='utf-8-sig')
        
        flash('새 수검자가 등록되었습니다.', 'success')
        return redirect(url_for('examinees_list'))
    
    clients_json = clients
    return render_template(
        'examinee_form.html',
        examinee={'source': default_source},
        clients=clients,
        clients_json=clients_json,
        status_choices=EXAMINEE_STATUS_CHOICES,
        source_choices=EXAMINEE_SOURCE_CHOICES,
        preselected_client_id=preselected_client_id,
        form_action=url_for('examinee_new'),
        submit_label='등록',
        is_edit=False,
        default_source=default_source
    )

@app.route('/examinees/<examinee_id>')
@login_required
def examinee_detail(examinee_id):
    examinees_df = get_examinees_dataframe()
    record = examinees_df[examinees_df['examinee_id'] == examinee_id]
    if record.empty:
        flash('수검자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('examinees_list'))
    
    examinee = record.fillna('').to_dict('records')[0]
    clients_df, clients = get_clients_records()
    linked_client = None
    if examinee.get('linked_client_id'):
        client_row = clients_df[clients_df['client_id'] == examinee['linked_client_id']]
        if not client_row.empty:
            linked_client = client_row.fillna('').to_dict('records')[0]
    
    associated_sessions = []
    if examinee.get('linked_client_id'):
        sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
        client_sessions = sessions_df[sessions_df['client_id'] == examinee['linked_client_id']]
        if not client_sessions.empty:
            client_sessions = client_sessions.sort_values('date', ascending=False).fillna('')
            associated_sessions = client_sessions.to_dict('records')
    
    return render_template(
        'examinee_detail.html',
        examinee=examinee,
        linked_client=linked_client,
        clients=clients,
        status_labels=dict(EXAMINEE_STATUS_CHOICES),
        source_labels=dict(EXAMINEE_SOURCE_CHOICES),
        status_choices=EXAMINEE_STATUS_CHOICES,
        source_choices=EXAMINEE_SOURCE_CHOICES,
        associated_sessions=associated_sessions
    )

@app.route('/examinees/<examinee_id>/edit', methods=['GET', 'POST'])
@login_required
def examinee_edit(examinee_id):
    examinees_df = get_examinees_dataframe()
    idx = examinees_df[examinees_df['examinee_id'] == examinee_id].index
    if len(idx) == 0:
        flash('수검자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('examinees_list'))
    
    row_index = idx[0]
    existing_data = examinees_df.loc[row_index].fillna('').to_dict()
    clients_df, clients = get_clients_records()
    
    if request.method == 'POST':
        linked_client_id = request.form.get('linked_client_id', '').strip()
        source = request.form.get('source', '').strip()
        status = request.form.get('status', '').strip()
        name = request.form.get('name', '').strip()
        birth_year = request.form.get('birth_year', '').strip()
        gender = request.form.get('gender', '').strip()
        phone = request.form.get('phone', '').strip()
        email = request.form.get('email', '').strip()
        assessment_date = request.form.get('assessment_date', '').strip()
        assessment_type = request.form.get('assessment_type', '').strip()
        assessment_tool = request.form.get('assessment_tool', '').strip()
        assessment_description = request.form.get('assessment_description', '').strip()
        notes = request.form.get('notes', '').strip()
        
        client_info = None
        if linked_client_id:
            client_row = clients_df[clients_df['client_id'] == linked_client_id]
            if client_row.empty:
                flash('선택한 내담자를 찾을 수 없습니다.', 'error')
                return redirect(url_for('examinee_edit', examinee_id=examinee_id))
            client_info = client_row.fillna('').to_dict('records')[0]
        
        if client_info:
            name = name or client_info.get('name', '')
            birth_year = birth_year or str(client_info.get('birth_year', '') or '')
            gender = gender or client_info.get('gender', '')
            phone = phone or client_info.get('phone', '')
            email = email or client_info.get('email', '')
        
        if not name:
            flash('수검자 이름을 입력해주세요.', 'error')
            return redirect(url_for('examinee_edit', examinee_id=examinee_id))
        
        if not source:
            source = 'existing_client' if linked_client_id else 'new_examinee'
        if not status:
            status = 'evaluating'
        
        report_file = request.files.get('report_file')
        report_filename = existing_data.get('report_file', '')
        if report_file and report_file.filename:
            file_ext = os.path.splitext(report_file.filename)[1].lower()
            if file_ext not in ALLOWED_PSYCH_TEST_EXTENSIONS:
                flash('허용되지 않는 파일 형식입니다. PDF, DOC, DOCX, TXT, JPG, PNG 파일만 업로드 가능합니다.', 'error')
                return redirect(url_for('examinee_edit', examinee_id=examinee_id))
            safe_name = secure_filename(report_file.filename)
            report_filename = f"{examinee_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{safe_name}"
            report_path = os.path.join(ASSESSMENT_FOLDER, report_filename)
            report_file.save(report_path)
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        examinees_df.at[row_index, 'name'] = name
        examinees_df.at[row_index, 'linked_client_id'] = linked_client_id
        examinees_df.at[row_index, 'source'] = source
        examinees_df.at[row_index, 'birth_year'] = birth_year
        examinees_df.at[row_index, 'gender'] = gender
        examinees_df.at[row_index, 'phone'] = phone
        examinees_df.at[row_index, 'email'] = email
        examinees_df.at[row_index, 'assessment_date'] = assessment_date
        examinees_df.at[row_index, 'assessment_type'] = assessment_type
        examinees_df.at[row_index, 'assessment_tool'] = assessment_tool
        examinees_df.at[row_index, 'assessment_description'] = assessment_description
        examinees_df.at[row_index, 'report_file'] = report_filename
        examinees_df.at[row_index, 'status'] = status
        examinees_df.at[row_index, 'notes'] = notes
        examinees_df.at[row_index, 'updated_at'] = timestamp
        
        examinees_df.to_csv(EXAMINEES_CSV, index=False, encoding='utf-8-sig')
        
        flash('수검자 정보가 수정되었습니다.', 'success')
        return redirect(url_for('examinee_detail', examinee_id=examinee_id))
    
    return render_template(
        'examinee_form.html',
        examinee=existing_data,
        clients=clients,
        clients_json=clients,
        status_choices=EXAMINEE_STATUS_CHOICES,
        source_choices=EXAMINEE_SOURCE_CHOICES,
        form_action=url_for('examinee_edit', examinee_id=examinee_id),
        submit_label='수정',
        is_edit=True,
        preselected_client_id=existing_data.get('linked_client_id', ''),
        default_source=existing_data.get('source', 'new_examinee')
    )

@app.route('/examinees/<examinee_id>/link-client', methods=['POST'])
@login_required
def examinee_link_client(examinee_id):
    client_id = request.form.get('client_id', '').strip()
    if not client_id:
        flash('연결할 내담자를 선택해주세요.', 'error')
        return redirect(url_for('examinee_detail', examinee_id=examinee_id))
    
    examinees_df = get_examinees_dataframe()
    idx = examinees_df[examinees_df['examinee_id'] == examinee_id].index
    if len(idx) == 0:
        flash('수검자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('examinees_list'))
    
    clients_df = get_clients_dataframe()
    client_row = clients_df[clients_df['client_id'] == client_id]
    if client_row.empty:
        flash('선택한 내담자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('examinee_detail', examinee_id=examinee_id))
    
    client_info = client_row.fillna('').to_dict('records')[0]
    row_index = idx[0]
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    examinees_df.at[row_index, 'linked_client_id'] = client_id
    examinees_df.at[row_index, 'source'] = 'existing_client'
    if not str(examinees_df.at[row_index, 'name']).strip():
        examinees_df.at[row_index, 'name'] = client_info.get('name', '')
    if not str(examinees_df.at[row_index, 'birth_year']).strip():
        examinees_df.at[row_index, 'birth_year'] = str(client_info.get('birth_year', '') or '')
    if not str(examinees_df.at[row_index, 'gender']).strip():
        examinees_df.at[row_index, 'gender'] = client_info.get('gender', '')
    if not str(examinees_df.at[row_index, 'phone']).strip():
        examinees_df.at[row_index, 'phone'] = client_info.get('phone', '')
    if not str(examinees_df.at[row_index, 'email']).strip():
        examinees_df.at[row_index, 'email'] = client_info.get('email', '')
    examinees_df.at[row_index, 'updated_at'] = timestamp
    
    examinees_df.to_csv(EXAMINEES_CSV, index=False, encoding='utf-8-sig')
    
    flash('수검자와 내담자가 연결되었습니다.', 'success')
    return redirect(url_for('examinee_detail', examinee_id=examinee_id))

@app.route('/examinees/<examinee_id>/create-client', methods=['POST'])
@login_required
def examinee_create_client(examinee_id):
    examinees_df = get_examinees_dataframe()
    idx = examinees_df[examinees_df['examinee_id'] == examinee_id].index
    if len(idx) == 0:
        flash('수검자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('examinees_list'))
    
    row_index = idx[0]
    examinee = examinees_df.loc[row_index].fillna('').to_dict()
    
    if examinee.get('linked_client_id'):
        flash('이미 내담자와 연결된 수검자입니다.', 'error')
        return redirect(url_for('examinee_detail', examinee_id=examinee_id))
    
    clients_df = get_clients_dataframe()
    new_client_id = generate_client_id()
    
    new_client = {
        'client_id': new_client_id,
        'name': examinee.get('name', ''),
        'phone': examinee.get('phone', ''),
        'email': examinee.get('email', ''),
        'birth_year': examinee.get('birth_year', ''),
        'gender': examinee.get('gender', ''),
        'first_session_date': '',
        'status': 'counseling',
        'reengaged_at': '',
        'tags': '',
        'notes': examinee.get('notes', ''),
        'medical_history': '',
        'counseling_history': '',
        'psychological_test_file': examinee.get('report_file', ''),
        'psychological_test_names': examinee.get('assessment_tool', '')
    }
    
    clients_df = pd.concat([clients_df, pd.DataFrame([new_client])], ignore_index=True)
    clients_df.to_csv(CLIENTS_CSV, index=False, encoding='utf-8-sig')
    
    examinees_df.at[row_index, 'linked_client_id'] = new_client_id
    examinees_df.at[row_index, 'source'] = 'existing_client'
    examinees_df.at[row_index, 'updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    examinees_df.to_csv(EXAMINEES_CSV, index=False, encoding='utf-8-sig')
    
    flash('새 내담자가 생성되었고 수검자와 연결되었습니다.', 'success')
    return redirect(url_for('client_detail', client_id=new_client_id))

@app.route('/examinees/<examinee_id>/delete', methods=['POST'])
@login_required
def examinee_delete(examinee_id):
    examinees_df = get_examinees_dataframe()
    idx = examinees_df[examinees_df['examinee_id'] == examinee_id].index
    if len(idx) == 0:
        flash('수검자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('examinees_list'))
    
    row_index = idx[0]
    examinee = examinees_df.loc[row_index].fillna('').to_dict()
    report_filename = examinee.get('report_file', '')
    
    examinees_df = examinees_df.drop(index=row_index)
    examinees_df.to_csv(EXAMINEES_CSV, index=False, encoding='utf-8-sig')
    
    if report_filename:
        file_path = os.path.join(ASSESSMENT_FOLDER, report_filename)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass
    
    flash('수검자 정보가 삭제되었습니다.', 'success')
    return redirect(url_for('examinees_list'))

@app.route('/download/examinee_report/<examinee_id>')
@login_required
def download_examinee_report(examinee_id):
    examinees_df = get_examinees_dataframe()
    record = examinees_df[examinees_df['examinee_id'] == examinee_id]
    if record.empty:
        flash('수검자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('examinees_list'))
    
    examinee = record.fillna('').to_dict('records')[0]
    filename = examinee.get('report_file', '')
    
    if not filename:
        flash('보고서 파일이 등록되어 있지 않습니다.', 'error')
        return redirect(url_for('examinee_detail', examinee_id=examinee_id))
    
    file_path = os.path.join(ASSESSMENT_FOLDER, filename)
    if not os.path.exists(file_path):
        flash('보고서 파일을 찾을 수 없습니다.', 'error')
        return redirect(url_for('examinee_detail', examinee_id=examinee_id))
    
    return send_file(file_path, as_attachment=True, download_name=filename)

@app.route('/clients/<client_id>/consents/new', methods=['GET', 'POST'])
@login_required
def consent_new(client_id):
    clients_df = get_clients_dataframe()
    client_row = clients_df[clients_df['client_id'] == client_id]
    if client_row.empty:
        flash('내담자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('clients_list'))

    consents_df = get_consents_dataframe()

    if request.method == 'POST':
        consent_id = generate_consent_id()
        consent_type = request.form.get('consent_type')
        status = request.form.get('status') or 'pending'
        signed_at = request.form.get('signed_at') or ''
        expires_at = request.form.get('expires_at') or ''
        notes = request.form.get('notes') or ''
        version = request.form.get('version') or 'v1'
        options = {}
        if consent_type == 'online_recording':
            options['record_audio'] = bool(request.form.get('record_audio'))
            options['record_video'] = bool(request.form.get('record_video'))

        valid_types = [value for value, _ in CONSENT_TYPES]
        if consent_type not in valid_types:
            flash('올바른 동의서 종류를 선택해주세요.', 'error')
            return redirect(url_for('consent_new', client_id=client_id))

        valid_status = [value for value, _ in CONSENT_STATUS_CHOICES]
        if status not in valid_status:
            status = 'pending'

        file_path = ''
        uploaded_file = request.files.get('consent_file')
        if uploaded_file and uploaded_file.filename:
            file_ext = os.path.splitext(uploaded_file.filename)[1].lower()
            if file_ext not in ALLOWED_PSYCH_TEST_EXTENSIONS:
                flash('허용되지 않는 파일 형식입니다. PDF, DOC, DOCX, TXT, JPG, PNG 파일만 업로드 가능합니다.', 'error')
                return redirect(url_for('consent_new', client_id=client_id))
            safe_name = secure_filename(uploaded_file.filename)
            file_path = f"{consent_id}_{safe_name}"
            uploaded_file.save(os.path.join(CONSENT_FOLDER, file_path))

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_consent = {
            'consent_id': consent_id,
            'client_id': client_id,
            'consent_type': consent_type,
            'status': status,
            'signed_at': signed_at,
            'expires_at': expires_at,
            'file_path': file_path,
            'options': json.dumps(options, ensure_ascii=False) if options else '',
            'notes': notes,
            'version': version,
            'created_at': timestamp,
            'updated_at': timestamp
        }

        consents_df = pd.concat([consents_df, pd.DataFrame([new_consent])], ignore_index=True)
        consents_df.to_csv(CONSENTS_CSV, index=False, encoding='utf-8-sig')

        flash('동의서가 등록되었습니다.', 'success')
        return redirect(url_for('client_detail', client_id=client_id))

    return render_template(
        'consent_form.html',
        client=client_row.fillna('').to_dict('records')[0],
        consent={},
        form_action=url_for('consent_new', client_id=client_id),
        submit_label='등록',
        consent_types=CONSENT_TYPES,
        consent_statuses=CONSENT_STATUS_CHOICES,
        is_edit=False
    )

@app.route('/clients/<client_id>/consents/<consent_id>/edit', methods=['GET', 'POST'])
@login_required
def consent_edit(client_id, consent_id):
    clients_df = get_clients_dataframe()
    client_row = clients_df[clients_df['client_id'] == client_id]
    if client_row.empty:
        flash('내담자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('clients_list'))

    consents_df = get_consents_dataframe()
    consent_idx = consents_df[consents_df['consent_id'] == consent_id].index
    if len(consent_idx) == 0:
        flash('동의서를 찾을 수 없습니다.', 'error')
        return redirect(url_for('client_detail', client_id=client_id))

    idx = consent_idx[0]
    existing = consents_df.loc[idx].fillna('').to_dict()

    if request.method == 'POST':
        consent_type = request.form.get('consent_type')
        status = request.form.get('status') or 'pending'
        signed_at = request.form.get('signed_at') or ''
        expires_at = request.form.get('expires_at') or ''
        notes = request.form.get('notes') or ''
        version = request.form.get('version') or existing.get('version') or 'v1'
        options = {}
        if consent_type == 'online_recording':
            options['record_audio'] = bool(request.form.get('record_audio'))
            options['record_video'] = bool(request.form.get('record_video'))

        valid_types = [value for value, _ in CONSENT_TYPES]
        if consent_type not in valid_types:
            flash('올바른 동의서 종류를 선택해주세요.', 'error')
            return redirect(url_for('consent_edit', client_id=client_id, consent_id=consent_id))

        valid_status = [value for value, _ in CONSENT_STATUS_CHOICES]
        if status not in valid_status:
            status = existing.get('status', 'pending')

        file_path = existing.get('file_path', '')
        uploaded_file = request.files.get('consent_file')
        if uploaded_file and uploaded_file.filename:
            file_ext = os.path.splitext(uploaded_file.filename)[1].lower()
            if file_ext not in ALLOWED_PSYCH_TEST_EXTENSIONS:
                flash('허용되지 않는 파일 형식입니다. PDF, DOC, DOCX, TXT, JPG, PNG 파일만 업로드 가능합니다.', 'error')
                return redirect(url_for('consent_edit', client_id=client_id, consent_id=consent_id))
            safe_name = secure_filename(uploaded_file.filename)
            file_path = f"{consent_id}_{safe_name}"
            uploaded_file.save(os.path.join(CONSENT_FOLDER, file_path))

        consents_df.at[idx, 'consent_type'] = consent_type
        consents_df.at[idx, 'status'] = status
        consents_df.at[idx, 'signed_at'] = signed_at
        consents_df.at[idx, 'expires_at'] = expires_at
        consents_df.at[idx, 'notes'] = notes
        consents_df.at[idx, 'version'] = version
        consents_df.at[idx, 'options'] = json.dumps(options, ensure_ascii=False) if options else ''
        consents_df.at[idx, 'file_path'] = file_path
        consents_df.at[idx, 'updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        consents_df.to_csv(CONSENTS_CSV, index=False, encoding='utf-8-sig')
        flash('동의서가 수정되었습니다.', 'success')
        return redirect(url_for('client_detail', client_id=client_id))

    options = json.loads(existing.get('options') or '{}')
    existing['record_audio'] = options.get('record_audio', False)
    existing['record_video'] = options.get('record_video', False)

    return render_template(
        'consent_form.html',
        client=client_row.fillna('').to_dict('records')[0],
        consent=existing,
        form_action=url_for('consent_edit', client_id=client_id, consent_id=consent_id),
        submit_label='수정',
        consent_types=CONSENT_TYPES,
        consent_statuses=CONSENT_STATUS_CHOICES,
        is_edit=True
    )

@app.route('/clients/<client_id>/consents/<consent_id>/delete', methods=['POST'])
@login_required
def consent_delete(client_id, consent_id):
    consents_df = get_consents_dataframe()
    consent_idx = consents_df[consents_df['consent_id'] == consent_id].index
    if len(consent_idx) == 0:
        flash('동의서를 찾을 수 없습니다.', 'error')
        return redirect(url_for('client_detail', client_id=client_id))
    idx = consent_idx[0]
    consent = consents_df.loc[idx].fillna('').to_dict()
    file_path = consent.get('file_path')
    consents_df = consents_df.drop(index=idx)
    consents_df.to_csv(CONSENTS_CSV, index=False, encoding='utf-8-sig')

    if file_path:
        full_path = os.path.join(CONSENT_FOLDER, file_path)
        if os.path.exists(full_path):
            try:
                os.remove(full_path)
            except OSError:
                pass

    flash('동의서가 삭제되었습니다.', 'success')
    return redirect(url_for('client_detail', client_id=client_id))

@app.route('/download/consent/<consent_id>')
@login_required
def download_consent(consent_id):
    consents_df = get_consents_dataframe()
    consent_row = consents_df[consents_df['consent_id'] == consent_id]
    if consent_row.empty:
        flash('동의서를 찾을 수 없습니다.', 'error')
        return redirect(url_for('clients_list'))
    consent = consent_row.fillna('').to_dict('records')[0]
    filename = consent.get('file_path', '')
    if not filename:
        flash('첨부 파일이 없습니다.', 'error')
        return redirect(url_for('client_detail', client_id=consent.get('client_id')))
    file_path = os.path.join(CONSENT_FOLDER, filename)
    if not os.path.exists(file_path):
        flash('파일을 찾을 수 없습니다.', 'error')
        return redirect(url_for('client_detail', client_id=consent.get('client_id')))
    return send_file(file_path, as_attachment=True, download_name=filename)

@app.route('/clients/<client_id>/consents/bootstrap')
@login_required
def consent_bootstrap(client_id):
    clients_df = get_clients_dataframe()
    if clients_df[clients_df['client_id'] == client_id].empty:
        flash('내담자를 찾을 수 없습니다.', 'error')
        return redirect(url_for('clients_list'))
    created = ensure_required_consents_for_client(client_id)
    if created:
        flash(f'필수 동의서 {created}건이 추가되었습니다.', 'success')
    else:
        flash('필수 동의서가 이미 모두 등록되어 있습니다.', 'info')
    return redirect(url_for('client_detail', client_id=client_id))

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
    
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
    
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
        
        sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
        
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
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
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
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
    clients_df = get_clients_dataframe()
    
    if not sessions_df.empty:
        sessions_df = annotate_sessions_with_participants(sessions_df, clients_df)
        sessions_df = sessions_df.sort_values('date', ascending=False)
        sessions_df['participant_ids'] = sessions_df['participant_ids'].apply(lambda ids: ','.join(ids) if isinstance(ids, list) else ids)
        sessions_df['participant_names'] = sessions_df['participant_names'].apply(lambda names: ', '.join(names) if isinstance(names, list) else names)
        cols = ['session_id', 'client_id', 'name', 'counseling_type', 'participant_ids', 'participant_names', 'participant_display'] + [
            col for col in sessions_df.columns
            if col not in ['session_id', 'client_id', 'name', 'counseling_type', 'participant_ids', 'participant_names', 'participant_display']
        ]
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
    
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
    
    if sessions_df.empty:
        current_year = datetime.now().year
        return render_template('monthly_revenue.html', 
                             year=current_year, 
                             years=[], 
                             monthly_stats=[],
                             payment_stats={},
                             total_revenue=0,
                             total_paid=0,
                             total_unpaid=0,
                             total_sessions_year=0,
                             total_paid_count=0,
                             total_unpaid_count=0,
                             yearly_stats=[])
    
    sessions_df['date'] = pd.to_datetime(sessions_df['date'], errors='coerce')
    sessions_df = sessions_df.dropna(subset=['date'])
    
    sessions_df['year'] = sessions_df['date'].dt.year
    sessions_df['month'] = sessions_df['date'].dt.month
    sessions_df['fee'] = pd.to_numeric(sessions_df['fee'], errors='coerce').fillna(0)
    
    available_years = sorted(sessions_df['year'].unique(), reverse=True)
    
    yearly_stats = []
    for y in available_years:
        yearly_sessions = sessions_df[sessions_df['year'] == y]
        paid_sessions_y = yearly_sessions[yearly_sessions['paid'] == 'Y']
        unpaid_sessions_y = yearly_sessions[yearly_sessions['paid'] == 'N']
        
        yearly_stats.append({
            'year': y,
            'total_revenue': int(yearly_sessions['fee'].sum()),
            'total_sessions': len(yearly_sessions),
            'paid_revenue': int(paid_sessions_y['fee'].sum()),
            'paid_count': len(paid_sessions_y),
            'unpaid_revenue': int(unpaid_sessions_y['fee'].sum()),
            'unpaid_count': len(unpaid_sessions_y)
        })
    
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
                'paid_count': 0,
                'unpaid_count': 0,
                'total_revenue': 0,
                'paid_revenue': 0,
                'unpaid_revenue': 0,
                'cash': 0,
                'card': 0,
                'voucher': 0,
                'free': 0,
                'cash_count': 0,
                'card_count': 0,
                'voucher_count': 0,
                'free_count': 0
            })
        else:
            paid_sessions = month_sessions[month_sessions['paid'] == 'Y']
            unpaid_sessions = month_sessions[month_sessions['paid'] == 'N']
            cash_sessions = month_sessions[month_sessions['payment_method'] == '현금']
            card_sessions = month_sessions[month_sessions['payment_method'] == '카드']
            voucher_sessions = month_sessions[month_sessions['payment_method'] == '바우처']
            free_sessions = month_sessions[month_sessions['payment_method'] == '무료']
            
            monthly_stats.append({
                'month': month,
                'total_sessions': len(month_sessions),
                'paid_count': len(paid_sessions),
                'unpaid_count': len(unpaid_sessions),
                'total_revenue': int(month_sessions['fee'].sum()),
                'paid_revenue': int(paid_sessions['fee'].sum()),
                'unpaid_revenue': int(unpaid_sessions['fee'].sum()),
                'cash': int(cash_sessions['fee'].sum()),
                'card': int(card_sessions['fee'].sum()),
                'voucher': int(voucher_sessions['fee'].sum()),
                'free': int(free_sessions['fee'].sum()),
                'cash_count': len(cash_sessions),
                'card_count': len(card_sessions),
                'voucher_count': len(voucher_sessions),
                'free_count': len(free_sessions)
            })
    
    def payment_summary(df, method):
        subset = df[df['payment_method'] == method]
        return {
            'amount': int(subset['fee'].sum()),
            'count': len(subset)
        }
    
    payment_stats = {
        'cash': payment_summary(year_sessions, '현금'),
        'card': payment_summary(year_sessions, '카드'),
        'voucher': payment_summary(year_sessions, '바우처'),
        'free': payment_summary(year_sessions, '무료')
    }
    
    total_revenue = int(year_sessions['fee'].sum())
    paid_sessions_year = year_sessions[year_sessions['paid'] == 'Y']
    unpaid_sessions_year = year_sessions[year_sessions['paid'] == 'N']
    total_paid = int(paid_sessions_year['fee'].sum())
    total_unpaid = int(unpaid_sessions_year['fee'].sum())
    total_sessions_year = len(year_sessions)
    total_paid_count = len(paid_sessions_year)
    total_unpaid_count = len(unpaid_sessions_year)
    
    return render_template('monthly_revenue.html',
                         year=year,
                         years=available_years,
                         monthly_stats=monthly_stats,
                         payment_stats=payment_stats,
                         total_revenue=total_revenue,
                         total_paid=total_paid,
                         total_unpaid=total_unpaid,
                         total_sessions_year=total_sessions_year,
                         total_paid_count=total_paid_count,
                         total_unpaid_count=total_unpaid_count,
                         yearly_stats=yearly_stats)

@app.route('/download/psychological_test/<client_id>')
@login_required
def download_psychological_test(client_id):
    clients_df = get_clients_dataframe()
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

@app.route('/calendar')
@login_required
def calendar_view():
    sessions_df = ensure_session_columns(pd.read_csv(SESSIONS_CSV, encoding='utf-8-sig'))
    clients_df = get_clients_dataframe()
    participants_df = get_session_participants_dataframe()
    counselor_events_df = get_counselor_events_dataframe()
    
    sessions_df = annotate_sessions_with_participants(sessions_df, clients_df, participants_df)
    sessions_df = sessions_df.fillna('')
    clients_map = clients_df.set_index('client_id')['name'].fillna('').to_dict() if not clients_df.empty else {}
    
    session_events = []
    for _, row in sessions_df.iterrows():
        date_str = str(row.get('date') or '').strip()
        if not date_str:
            continue
        try:
            start_dt = pd.to_datetime(date_str)
        except Exception:
            continue
        duration = int(row.get('duration_minutes') or 0) or 50
        end_dt = start_dt + pd.Timedelta(minutes=duration)
        participants = row.get('participant_names') or []
        title_parts = []
        if participants:
            title_parts.append(', '.join(participants))
        if row.get('mode'):
            title_parts.append(f"({row['mode']})")
        title = ' '.join(title_parts).strip()
        if not title:
            title = f"회기 {row.get('session_id')}"
        if not str(title).startswith('🗓'):
            title_display = f"🗓 {title}"
        else:
            title_display = title
        session_events.append({
            'id': row.get('session_id'),
            'title': title_display,
            'start': start_dt.isoformat(),
            'end': end_dt.isoformat(),
            'allDay': False,
            'category': 'session',
            'mode': row.get('mode', ''),
            'participants': participants,
            'fee': row.get('fee', ''),
            'status': row.get('paid', ''),
            'url': url_for('session_detail', session_id=row.get('session_id'))
        })
    
    counselor_events_df = counselor_events_df.fillna('')
    personal_events = []
    for _, row in counselor_events_df.iterrows():
        start = str(row.get('start') or '').strip()
        end = str(row.get('end') or '').strip()
        if not start:
            continue
        title = row.get('title') or '상담자 일정'
        if not str(title).startswith('⭐'):
            title_display = f"⭐ {title}"
        else:
            title_display = title
        personal_events.append({
            'id': row.get('event_id') or generate_event_id(),
            'title': title_display,
            'start': start,
            'end': end or None,
            'allDay': parse_bool(row.get('all_day')),
            'category': row.get('category') or 'personal',
            'counselor': row.get('counselor') or '',
            'description': row.get('description') or '',
            'location': row.get('location') or ''
        })
    
    events = session_events + personal_events
    
    return render_template(
        'calendar.html',
        events_json=json.dumps(events, ensure_ascii=False),
        clients=clients_map
    )

@app.route('/calendar/events')
@login_required
def counselor_events_list():
    events_df = get_counselor_events_dataframe().fillna('')
    if not events_df.empty:
        events_df['start_sort'] = pd.to_datetime(events_df['start'], errors='coerce')
        events_df = events_df.sort_values(['start_sort', 'title'])
    events = events_df.drop(columns=['start_sort'], errors='ignore').to_dict('records')
    return render_template('counselor_events_list.html', events=events)

def build_event_form_data(event_row=None):
    event = {
        'event_id': '',
        'title': '',
        'start': '',
        'end': '',
        'start_local': '',
        'end_local': '',
        'all_day': '',
        'all_day_checked': False,
        'category': 'personal',
        'counselor': '',
        'location': '',
        'description': ''
    }
    if event_row:
        event.update(event_row)
    event['start_local'] = to_datetime_local(event.get('start'))
    event['end_local'] = to_datetime_local(event.get('end'))
    event['all_day_checked'] = parse_bool(event.get('all_day'))
    return event

@app.route('/calendar/events/new', methods=['GET', 'POST'])
@login_required
def counselor_event_new():
    if request.method == 'POST':
        title = (request.form.get('title') or '').strip() or '상담자 일정'
        start = (request.form.get('start') or '').strip()
        end = (request.form.get('end') or '').strip()
        all_day = 'all_day' in request.form
        if not start:
            flash('시작 일시를 입력해주세요.', 'error')
            return redirect(url_for('counselor_event_new'))
        if not end:
            end = ''
        overlap_messages = []
        start_dt = pd.to_datetime(start, errors='coerce')
        end_dt = pd.to_datetime(end, errors='coerce') if end else None
        if pd.isna(start_dt):
            flash('시작 일시 형식이 올바르지 않습니다.', 'error')
            return redirect(url_for('counselor_event_new'))
        if end and pd.isna(end_dt):
            flash('종료 일시 형식이 올바르지 않습니다.', 'error')
            return redirect(url_for('counselor_event_new'))
        new_range_start, new_range_end = prepare_event_range(start_dt, end_dt, all_day, DEFAULT_EVENT_DURATION_MINUTES)
        if new_range_start is None or new_range_end is None:
            flash('일정 시간을 확인해주세요.', 'error')
            return redirect(url_for('counselor_event_new'))
        counselor_name = request.form.get('counselor')
        overlap_messages.extend(find_overlapping_sessions(new_range_start, new_range_end, counselor_name))
        overlap_messages.extend(find_overlapping_personal_events(new_range_start, new_range_end, counselor_name))
        if overlap_messages:
            for msg in overlap_messages:
                flash(msg, 'error')
            return redirect(url_for('counselor_event_new'))
        events_df = get_counselor_events_dataframe()
        new_event = {
            'event_id': generate_event_id(),
            'title': title,
            'description': (request.form.get('description') or '').strip(),
            'start': start,
            'end': end,
            'all_day': 'True' if all_day else 'False',
            'category': (request.form.get('category') or 'personal').strip(),
            'counselor': (request.form.get('counselor') or '').strip(),
            'location': (request.form.get('location') or '').strip()
        }
        events_df = ensure_counselor_event_columns(events_df)
        events_df = pd.concat([events_df, pd.DataFrame([new_event])], ignore_index=True)
        events_df.to_csv(COUNSELOR_EVENTS_CSV, index=False, encoding='utf-8-sig')
        flash('상담자 일정이 등록되었습니다.', 'success')
        return redirect(url_for('counselor_events_list'))
    
    event = build_event_form_data()
    default_date = request.args.get('date')
    if default_date:
        event['start_local'] = default_date if 'T' in default_date else f"{default_date}T09:00"
        event['end_local'] = ''
    return render_template(
        'counselor_event_form.html',
        event=event,
        form_action=url_for('counselor_event_new'),
        submit_label='등록',
        is_edit=False
    )

@app.route('/calendar/events/<event_id>/edit', methods=['GET', 'POST'])
@login_required
def counselor_event_edit(event_id):
    events_df = get_counselor_events_dataframe()
    event_row = events_df[events_df['event_id'] == event_id]
    if event_row.empty:
        flash('일정을 찾을 수 없습니다.', 'error')
        return redirect(url_for('counselor_events_list'))
    
    if request.method == 'POST':
        title = (request.form.get('title') or '').strip() or '상담자 일정'
        start = (request.form.get('start') or '').strip()
        end = (request.form.get('end') or '').strip()
        all_day = 'all_day' in request.form
        if not start:
            flash('시작 일시를 입력해주세요.', 'error')
            return redirect(url_for('counselor_event_edit', event_id=event_id))
        if not end:
            end = ''
        start_dt = pd.to_datetime(start, errors='coerce')
        end_dt = pd.to_datetime(end, errors='coerce') if end else None
        if pd.isna(start_dt):
            flash('시작 일시 형식이 올바르지 않습니다.', 'error')
            return redirect(url_for('counselor_event_edit', event_id=event_id))
        if end and pd.isna(end_dt):
            flash('종료 일시 형식이 올바르지 않습니다.', 'error')
            return redirect(url_for('counselor_event_edit', event_id=event_id))
        new_range_start, new_range_end = prepare_event_range(start_dt, end_dt, all_day, DEFAULT_EVENT_DURATION_MINUTES)
        if new_range_start is None or new_range_end is None:
            flash('일정 시간을 확인해주세요.', 'error')
            return redirect(url_for('counselor_event_edit', event_id=event_id))
        counselor_name = request.form.get('counselor')
        overlap_messages = []
        overlap_messages.extend(find_overlapping_sessions(new_range_start, new_range_end, counselor_name))
        overlap_messages.extend(find_overlapping_personal_events(new_range_start, new_range_end, counselor_name, exclude_event_id=event_id))
        if overlap_messages:
            for msg in overlap_messages:
                flash(msg, 'error')
            return redirect(url_for('counselor_event_edit', event_id=event_id))
        idx = event_row.index[0]
        events_df.at[idx, 'title'] = title
        events_df.at[idx, 'description'] = (request.form.get('description') or '').strip()
        events_df.at[idx, 'start'] = start
        events_df.at[idx, 'end'] = end
        events_df.at[idx, 'all_day'] = 'True' if all_day else 'False'
        events_df.at[idx, 'category'] = (request.form.get('category') or 'personal').strip()
        events_df.at[idx, 'counselor'] = (request.form.get('counselor') or '').strip()
        events_df.at[idx, 'location'] = (request.form.get('location') or '').strip()
        events_df.to_csv(COUNSELOR_EVENTS_CSV, index=False, encoding='utf-8-sig')
        flash('상담자 일정이 수정되었습니다.', 'success')
        return redirect(url_for('counselor_events_list'))
    
    event = build_event_form_data(event_row.fillna('').to_dict('records')[0])
    return render_template(
        'counselor_event_form.html',
        event=event,
        form_action=url_for('counselor_event_edit', event_id=event_id),
        submit_label='수정',
        is_edit=True
    )

@app.route('/calendar/events/<event_id>/delete', methods=['POST'])
@login_required
def counselor_event_delete(event_id):
    events_df = get_counselor_events_dataframe()
    if events_df.empty or event_id not in events_df['event_id'].values:
        flash('일정을 찾을 수 없습니다.', 'error')
        return redirect(url_for('counselor_events_list'))
    events_df = events_df[events_df['event_id'] != event_id]
    events_df.to_csv(COUNSELOR_EVENTS_CSV, index=False, encoding='utf-8-sig')
    flash('상담자 일정이 삭제되었습니다.', 'success')
    return redirect(url_for('counselor_events_list'))

if __name__ == '__main__':
    init_csv_files()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
