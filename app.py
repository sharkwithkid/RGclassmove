# app.py (Streamlit)
from __future__ import annotations

from datetime import date
<<<<<<< HEAD
<<<<<<< HEAD
from pathlib import Path
from typing import Optional, List
import re
=======
import re
from pathlib import Path
from typing import Optional, List
>>>>>>> f3aadff (Initial commit)
=======
from pathlib import Path
from typing import Optional, List
import re
>>>>>>> d9c3029 (Add files via upload)

import streamlit as st

from core.utils import text_contains

from core.pipeline import (
    scan_work_root,
    scan_pipeline,
    run_pipeline,
    search_schools_in_db,
    get_school_domain_from_db,
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)
    domain_missing_message,
    detect_input_layout,  # 자동 레이아웃 감지
    get_project_dirs,
    load_notice_templates,
)

st.set_page_config(page_title="리딩게이트 반편성", layout="wide")

LOG_PATTERN = re.compile(r"\[(\w+)\]\s*(.*)")


def split_log_level(line: str) -> tuple[str, str]:
    """
    예: "[WARN] 학생명부 파일명 학년도(2024)가 ..."
    -> ("WARN", "학생명부 파일명 학년도(2024)가 ...")
    """
    m = LOG_PATTERN.match(line)
    if not m:
        return "INFO", line
    return m.group(1), m.group(2)


def get_all_school_names(work_root_str: str) -> List[str]:
    if not work_root_str:
        return []

    root = Path(work_root_str)
    names: set[str] = set()

    for kw in ("초", "중", "고"):
        # search_schools_in_db 시그니처가 바뀌었을 수도 있으니 둘 다 지원
        try:
            results = search_schools_in_db(root, kw, limit=10000)
        except TypeError:
            # limit 인자를 안 받는 버전인 경우
            results = search_schools_in_db(root, kw)

        for s in results:
            names.add(s)

    return sorted(names)

<<<<<<< HEAD
=======
)


st.set_page_config(page_title="리딩게이트 반이동 자동화", layout="wide")


# -------------------------
# 학교 안내 템플릿 (내부 고정)
# -------------------------
NOTICE_TEMPLATES = {
    '신규등록 - 메일': '[리딩게이트] OO초 - 2026학년도 신입생, 전입생, 교직원 등록 완료 안내\n\n안녕하세요. 리딩게이트입니다.\n\n2026학년도 신규 사용자 등록이 완료되어 안내드립니다.\nID, PW를 포함한 등록 명단을 파일로 첨부하였으니 확인 부탁드립니다.\n(학생, 교직원 모두 오늘부터 바로 이용 가능합니다.)\n\n신규로 발급한 계정 중 동명이인은 구분자(A, B)를 넣어 표시하였으니, \n반드시 본인 ID로 로그인하여 이용할 수 있도록 안내해 주시기 바랍니다.\n\nOO초 전용 리딩게이트 홈페이지 주소는 OOOOO.readinggate.com 입니다.\n초기 비밀번호로 로그인한 후 비밀번호를 변경해야 프로그램을 이용할 수 있습니다.\n\n※ 선생님들의 관리용 아이디, 비밀번호는 모두 초기화되었습니다.\n (관리용 아이디 : 선생님 이름 / 초기 비밀번호 : t1234)\n\n기존에 계시던 선생님과 재학생의 학습용 아이디는 사용하던 ID, PW 그대로 사용하시면 됩니다.\n또한 기존 재학생의 경우 3월 4일부터 로그인 후 직접 반 선택이 가능하니 자세한 내용은 이전에 보내드린 메일 확인 부탁드립니다.\n\n※ 이전 메일 제목 : 2026학년도 신규 아이디 등록 및 진급 학년/반 이동을 위한 안내 자료\n\n검토 후 문의사항 있으시면 연락 주세요.\n감사합니다.\n\n-----------------------------------------------------------------------------------',
    '신규등록 - 문자': '[리딩게이트]\n안녕하세요 선생님^^\n보내주신 2026학년도 사용자 자료 확인하여 등록을 완료하였습니다.\n등록 명단을 메일로 보내드렸으니 확인 부탁드립니다.\n감사합니다.\n\n-----------------------------------------------------------------------------------',
    '교직원만 등록 - 메일': '제목 : \n[리딩게이트] OO초 교직원 등록 완료 안내\n\n내용 :\n안녕하세요. 리딩게이트입니다.\n보내주신 교직원 명단 확인하여 등록을 완료하였습니다.\n등록 명단을 파일로 첨부드리니 ID/PW 정보를 첨부파일에서 확인해 주세요.\n감사합니다.\n\n-----------------------------------------------------------------------------------',
    '반이동 - 메일': '[리딩게이트] OO초 - 2026학년도 2-6학년 반이동 완료 안내  \n\n안녕하세요. 리딩게이트입니다.\n\n2026학년도 2-6학년의 반이동 작업이 완료되어 작업 명단을 파일로 첨부 드렸습니다.\n\n기존 재학생은 사용하던 ID, PW를 그대로 사용하면 됩니다.\n신규 등록 학생은 첨부 파일에 ID, PW 기재 후 색으로 표시하였으니\n본인 아이디로 로그인하여 학습할 수 있도록 안내해 주세요.\n\n검토 후 문의사항 있으시면 연락 주세요.\n감사합니다.\n\n-----------------------------------------------------------------------------------',
    '반이동 - 메일 (신입생, 교직원 등록 & 반이동)': '[리딩게이트] OO초 - 2026학년도 신입생, 교직원 등록 및 2-6학년 반이동 완료 안내  \n\n안녕하세요. 리딩게이트입니다.\n\n2026학년도 신입생 및 교직원 등록과 2~6학년 반이동 작업이 완료되어 안내드립니다.\n\n등록 명단을 파일로 첨부하였으니 확인 부탁드립니다.\n\n기존 재학생은 사용하던 ID, PW를 그대로 사용하면 됩니다.\n신규 등록 학생은 첨부 파일에 ID, PW 기재 후 색으로 표시하였으니\n본인 아이디로 로그인하여 학습할 수 있도록 안내해 주세요.\n\n(신규로 발급한 계정 중 동명이인은 구분자(A, B)를 넣어 표시하였으니, \n반드시 본인 ID로 로그인하여 이용할 수 있도록 안내해 주시기 바랍니다.)\n\nOO초 전용 리딩게이트 홈페이지 주소는 OOOOO.readinggate.com 입니다.\n초기 비밀번호로 로그인한 후에는 비밀번호를 변경해야 프로그램을 이용할 수 있습니다.\n\n※ 선생님들의 관리용아이디는 비밀번호 모두 초기화되었습니다.\n (관리용아이디 : 선생님이름,  초기비밀번호 : t1234)\n\n검토 후 문의사항 있으시면 연락 주세요.\n감사합니다.\n\n-----------------------------------------------------------------------------------',
    '반이동 - 문자': '[리딩게이트]\n안녕하세요. 선생님^^\n2026학년도 반이동 작업이 완료되어 내용을 메일로 보내드렸으니 확인 부탁드립니다.\n감사합니다.\n\n-----------------------------------------------------------------------------------',
    '2-6학년 명단 보내 온 경우 - 메일': '안녕하세요. 리딩게이트입니다.\n보내주신 2026학년도 사용자 자료 확인하여 메일드립니다.\n\n2-6학년 기존 학생들의 경우,\n3월 4일부터 로그인 후 2026학년도 본인의 학년과 반을 직접 선택하게 됩니다.\n또한 기존에 사용하던 ID와 PW, 획득한 포인트, 학습 이력 등은 그대로 유지됩니다.\n\n따라서 보내주신 자료에서 2-6학년 기존 학생을 제외한 나머지 사용자만 등록한 후 다시 메일 드리겠습니다.\n\n감사합니다.\n\n※ 만약 2-6학년의 반 편성을 리딩게이트에 요청하시려면, \n나이스에서 이전반(2025학년도) 기준으로 2026학년도 2-6학년의 명단을 다운로드하여 보내주시기 바랍니다.\n\n-----------------------------------------------------------------------------------',
    '2-6학년 반편성 자료 재요청 - 문자': '[리딩게이트]\n안녕하세요. 선생님^^\n\n메일로 보내주신 명단에 2-6학년 반편성에 필요한 필수 항목이 누락되어 안내드립니다.\n(학생 이름 / 이전반(2025학년도) / 이후반(2026학년도) 정보 필요) \n\n나이스(NEIS)에서 이전반, 이후반이 함께 표시된 진급 학적 명렬표를 엑셀파일로 다운로드하여 보내주시기 바랍니다.\n\n※ 메일 보내실 곳 : readinggate@readinggate.com\n\n자료를 다시 보내주시면 작업 완료 후 연락드리겠습니다.',
}

NOTICE_ORDER = ['신규등록 - 메일', '신규등록 - 문자', '교직원만 등록 - 메일', '반이동 - 메일', '반이동 - 메일 (신입생, 교직원 등록 & 반이동)', '반이동 - 문자', '2-6학년 명단 보내 온 경우 - 메일', '2-6학년 반편성 자료 재요청 - 문자']


def _render_notice_text(template: str, school_name: str, domain: str | None) -> str:
    """학교명/도메인 치환 후 안내문 반환"""
    t = template.replace("OO초", school_name).replace("OO중", school_name).replace("OO고", school_name)

    if domain:
        t = t.replace("OOOOO.readinggate.com", domain)
    else:
        # 도메인이 없으면, 도메인 문장 자체를 오류 문장으로 대체
        err_line = f"{school_name} (사용자가 작업중인) 의 도메인 주소가 존재하지 않습니다. 학교 전체 명단 파일을 확인하세요."
        # 해당 문장 패턴을 최대한 안전하게 교체
        t = re.sub(r"^.*전용 리딩게이트 홈페이지 주소는 .*readinggate\.com 입니다\.$", err_line, t, flags=re.M)
        # 혹시 패턴 매칭이 안 되면 토큰만 교체
        t = t.replace("OOOOO.readinggate.com", err_line)

    return t.strip() + "\n"
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)

# -------------------------
# session init
# -------------------------
ss = st.session_state
ss.setdefault("work_root", "")
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)
ss.setdefault("path_scan", None)
ss.setdefault("config_applied", False)
ss.setdefault("open_date", date.today())

# DB / 안내문 치환용 학교명
ss.setdefault("school_selected", "")
# 실제 폴더 이름 (예: 성남판교대장초_임지윤)
ss.setdefault("school_folder_name", "")
ss.setdefault("school_ok", False)

ss.setdefault("scan", None)
ss.setdefault("scan_ok", False)

# 레이아웃(데이터 시작 행) 상태
# kind -> {"header_row": int, "data_start_row": int, "example_rows": List[int]}
ss.setdefault("layout_overrides", {})

ss.setdefault("run_result", None)
ss.setdefault("run_logs", [])

# 작업일 (파이프라인 work_date)
ss.setdefault("work_date", date.today())

# 명부 기준일 (학생명부 마지막 수정일 기준)
ss.setdefault("roster_basis_date", None)
ss.setdefault("roster_basis_draft", None)  # 적용 전 임시 값

## 작업 폴더 적용 여부 플래그
ss.setdefault("work_root_applied", False)

# 안내문 텍스트 자동 갱신 기준 (학교 / 개학일)
ss.setdefault("notice_last_school", None)
ss.setdefault("notice_last_open_date", None)


<<<<<<< HEAD
=======
ss.setdefault("path_scan", None)          # dict from scan_work_root
ss.setdefault("config_applied", False)
ss.setdefault("open_date", date(2026, 2, 16))

ss.setdefault("school_keyword", "")
ss.setdefault("school_selected", "")
ss.setdefault("school_ok", False)

ss.setdefault("scan", None)               # ScanResult from scan_pipeline
ss.setdefault("scan_ok", False)

ss.setdefault("run_result", None)         # PipelineResult
ss.setdefault("run_logs", [])

>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
# -------------------------
# helpers
# -------------------------
def box_errors(msgs: List[str]):
    for m in msgs:
        st.error(m)

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)

def box_success(msg: str):
    st.success(msg)


def box_warn(msg: str):
    st.warning(msg)


def fmt_path(p: Optional[Path]) -> str:
    return "-" if p is None else str(p)


def init_default_layout_from_scan(ss):
    """
    scan 결과 + detect_input_layout 기반으로
    각 파일 유형별 header_row / data_start_row / example_rows 기본값을 세팅한다.
    """
    scan = ss.get("scan")
    if scan is None or not getattr(scan, "ok", False):
        ss.layout_overrides = {}
        return

    layout: dict[str, dict] = {}

    def _fallback(kind: str):
        if kind == "freshmen":
            layout[kind] = {"header_row": 4, "data_start_row": 5, "example_rows": []}
        elif kind == "transfer":
            layout[kind] = {"header_row": 2, "data_start_row": 5, "example_rows": []}
        elif kind == "withdraw":
            layout[kind] = {"header_row": 2, "data_start_row": 5, "example_rows": []}
        elif kind == "teacher":
            layout[kind] = {"header_row": 3, "data_start_row": 4, "example_rows": []}

    # 각 kind별 파일이 있으면 detect_input_layout 호출
    for kind, attr, default_header, default_data in [
        ("freshmen", "freshmen_file", 4, 5),
        ("transfer", "transfer_file", 2, 5),
        ("withdraw", "withdraw_file", 2, 5),
        ("teacher", "teacher_file", 3, 4),
    ]:
        p: Optional[Path] = getattr(scan, attr, None)
        if not p:
            continue

        try:
            info = detect_input_layout(p, kind)
            layout[kind] = {
                "header_row": int(info.get("header_row", default_header)),
                "data_start_row": int(info.get("data_start_row", default_data)),
                "example_rows": list(info.get("example_rows", [])),
            }
        except Exception:
            _fallback(kind)

    ss.layout_overrides = layout


<<<<<<< HEAD
=======
def box_success(msg: str):
    st.success(msg)

def box_warn(msg: str):
    st.warning(msg)

def fmt_path(p: Optional[Path]) -> str:
    return "-" if p is None else str(p)

>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
# -------------------------
# Header
# -------------------------
st.title("리딩게이트 반편성")

# ============================================================
# 1) 기본 설정(최초 1회)
# ============================================================
st.header("기본 설정 (최초 1회)")

st.subheader("작업 폴더")
st.caption(
    "작업 폴더 안에는 반드시 resources 폴더가 있어야 하며, "
    "그 안에 DB / 양식 / 안내문 파일이 모두 들어 있어야 합니다."
)

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)
st.markdown("**폴더 경로**")
col1, col2 = st.columns([4, 1])
with col1:
    ss.work_root = st.text_input(
        label="",
        value=ss.work_root,
        placeholder="/Users/.../2026반이동",
        label_visibility="collapsed",
    )
with col2:
    if st.button("경로 적용", use_container_width=True, key="btn_apply_work_root"):
<<<<<<< HEAD
=======
col1, col2 = st.columns([4, 1])
with col1:
    ss.work_root = st.text_input("폴더 경로", value=ss.work_root, placeholder="/Users/.../2026반이동")
with col2:
    if st.button("경로 적용", use_container_width=True):
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
        if not ss.work_root.strip():
            ss.path_scan = {"ok": False, "errors": ["[오류] 작업 폴더 경로가 비어 있습니다."]}
            ss.work_root_applied = False
        else:
            # 한 번만 스캔해서 결과를 세션에 보관
            ss.path_scan = scan_work_root(Path(ss.work_root))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
            ss.work_root_applied = bool(ss.path_scan.get("ok", False))

>>>>>>> 0f1c56e (20260227)
        # 설정/상태 초기화
=======
        # 경로 바뀌면 이후 단계 초기화
>>>>>>> f3aadff (Initial commit)
=======
        # 설정/상태 초기화
>>>>>>> d9c3029 (Add files via upload)
        ss.config_applied = False
        ss.school_selected = ""
        ss.school_folder_name = ""
        ss.school_ok = False
        ss.scan = None
        ss.scan_ok = False
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)
        ss.layout_overrides = {}
        ss.run_result = None
        ss.run_logs = []

<<<<<<< HEAD
if ss.path_scan is not None:
<<<<<<< HEAD
=======
        ss.run_result = None
        ss.run_logs = []

# 경로 적용 결과 표시
if ss.path_scan is not None:
    # DB 상태
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
    if not ss.path_scan.get("db_ok", False):
        box_errors(ss.path_scan.get("errors_db", ["[DB] 폴더/파일을 확인해주세요."]))
    else:
        dbf = ss.path_scan.get("db_file")
        st.write(f"[DB] {dbf.name if dbf else '-'}")

<<<<<<< HEAD
<<<<<<< HEAD
=======
    # 양식(templates) 상태
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
    if not ss.path_scan.get("format_ok", False):
        box_errors(ss.path_scan.get("errors_format", ["[양식] 폴더/파일을 확인해주세요."]))
    else:
        reg_t = ss.path_scan.get("register_template")
        notice_t = ss.path_scan.get("notice_template")
        st.write(f"[양식] {reg_t.name if reg_t else '-'}")
        st.write(f"[양식] {notice_t.name if notice_t else '-'}")

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)
st.subheader("개학일")
col_d1, col_d2 = st.columns([4, 1])
=======
# DB / 양식 상태 표시
if ss.work_root_applied and ss.work_root and ss.path_scan:
    scan_info = ss.path_scan

    missing_msgs = []

    # 1) DB 쪽 문제
    if not scan_info.get("db_ok", False):
        missing_msgs.append("resources / DB 폴더를 확인해 주세요.")

    # 2) 양식(templates) 쪽 문제
    if not scan_info.get("format_ok", False):
        missing_msgs.append("resources / templates 폴더를 확인해 주세요.")

    # 3) 안내문(notices) 쪽 문제
    notice_titles = scan_info.get("notice_titles", [])
    if not notice_titles:
        missing_msgs.append("resources / notices 폴더를 확인해 주세요.")

    # 에러 박스 구성
    if missing_msgs:
        full_msg = "필수 자료가 누락되었습니다. " + " ".join(missing_msgs)
        st.error(full_msg)

# 날짜 두 개를 같은 섹션 안에 붙여둠
st.subheader("개학일 / 작업일")

col_d1, col_d2 = st.columns(2)
>>>>>>> 0f1c56e (20260227)
with col_d1:
    ss.open_date = st.date_input(
        label="개학일",
        value=ss.open_date,
        key="open_date_input",
    )
with col_d2:
    ss.work_date = st.date_input(
        label="작업일",
        value=ss.work_date,
        key="work_date_input",
    )

st.markdown("")

# 설정 저장: 리소스 폴더 조건 통과했을 때만 활성화 (기존 로직 유지)
can_apply = ss.path_scan is not None and ss.path_scan.get("ok", False)
if st.button(
    "설정 저장",
    use_container_width=True,
    disabled=not can_apply,
    key="btn_config_save",
):
    ss.config_applied = True
    box_success("설정이 저장되었습니다.")
<<<<<<< HEAD
=======

st.subheader("개학일")
ss.open_date = st.date_input("개학일 입력", value=ss.open_date)

# 설정 적용
can_apply = ss.path_scan is not None and ss.path_scan.get("ok", False)
if st.button("설정 적용", use_container_width=True, disabled=not can_apply):
    ss.config_applied = True
    box_success("설정이 적용되었습니다.")
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)

st.divider()

# ============================================================
# 2) 학교 선택
# ============================================================
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)
st.header("학교 선택")
st.caption("작업 폴더 안에 있는 학교 폴더 이름은 DB에 등록된 학교명과 동일해야 합니다.")

# 기본 설정이 끝났는지 + 경로 스캔이 정상인지 둘 다 확인
if not (ss.config_applied and ss.path_scan and ss.path_scan.get("ok", False)):
    box_warn("먼저 공통 설정을 저장해 주세요.")
else:
    work_root = Path(ss.work_root).resolve()
    all_schools = get_all_school_names(str(work_root))

    if not all_schools:
        box_warn("DB에서 학교 목록을 불러오지 못했습니다. [DB] 폴더와 학교 전체 명단 파일을 확인해 주세요.")
    else:
        st.markdown("**학교**")
        col_s1, col_s2 = st.columns([4, 1])
        with col_s1:
            if ss.school_selected and ss.school_selected in all_schools:
                current_index: Optional[int] = all_schools.index(ss.school_selected)
            else:
                current_index = None

            selected_name = st.selectbox(
                label="",
                options=all_schools,
                index=current_index,
                placeholder="학교명을 입력하거나 선택하세요",
                label_visibility="collapsed",
                key="school_selectbox",
            )
        with col_s2:
            apply_clicked = st.button("선택", use_container_width=True, key="btn_school_select")

        if apply_clicked:
            if not selected_name:
                ss.school_ok = False
                ss.school_selected = ""
                ss.school_folder_name = ""
                box_warn("학교를 선택해 주세요.")
            else:
                name = selected_name.strip()

<<<<<<< HEAD
<<<<<<< HEAD
=======
st.header("학교")
st.caption("폴더 이름은 DB에 등록된 학교명과 동일해야 합니다.")

if not ss.config_applied:
    box_warn("먼저 [기본 설정]에서 경로를 적용하고 설정을 적용해 주세요.")
else:
    work_root = Path(ss.work_root).resolve()

    # DB 자동완성(키워드 검색)
    colA, colB = st.columns([3, 1])
    with colA:
        ss.school_keyword = st.text_input("학교명 입력", value=ss.school_keyword, placeholder="예: 세종한솔초")
    with colB:
        if st.button("학교 선택", use_container_width=True):
            name = (ss.school_keyword or "").strip()
            if not name:
                ss.school_ok = False
                ss.school_selected = ""
                st.error("학교명이 비어 있습니다.")
            else:
                # 학교 폴더 존재 확인
                school_dir = work_root / name
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
                if not school_dir.exists():
=======
                # 🔹 work_root 아래 실제 학교 폴더들 (resources, 숨김 폴더 제외)
                school_dirs = [
                    p for p in work_root.iterdir()
                    if p.is_dir()
                    and "resources" not in p.name.lower()
                    and not p.name.startswith(".")
                ]

                # 🔹 포함 매칭: 폴더명 안에 선택한 학교명이 들어가면 후보로
                matched = [
                    p for p in school_dirs
                    if text_contains(p.name, name)
                ]

                if not matched:
>>>>>>> 0f1c56e (20260227)
                    ss.school_ok = False
                    ss.school_selected = ""
                    ss.school_folder_name = ""
                    st.error("설정한 작업 폴더 안에 해당 학교 폴더가 없습니다. 폴더를 생성한 후 다시 시도해 주세요.")
                elif len(matched) > 1:
                    ss.school_ok = False
                    ss.school_selected = ""
                    ss.school_folder_name = ""
                    st.error(
                        f"'{name}' 이(가) 포함된 학교 폴더가 여러 개입니다: "
                        + ", ".join(p.name for p in matched)
                    )
                else:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
                    # DB 존재 확인은 scan_pipeline에서 한 번 더 검증됨
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
                    ss.school_selected = name
=======
                    folder_name = matched[0].name  # 실제 폴더명

                    # ✅ DB용 이름 / 실제 폴더 이름 따로 저장
                    ss.school_selected = name           # DB / 안내문 치환용
                    ss.school_folder_name = folder_name # 실제 폴더 접근용
>>>>>>> 0f1c56e (20260227)
                    ss.school_ok = True

                    # 학교 바뀌면 스캔/실행 상태 초기화
                    ss.scan = None
                    ss.scan_ok = False
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)
                    ss.layout_overrides = {}
                    ss.run_result = None
                    ss.run_logs = []

                    # 🔹 안내문용 기준값도 같이 초기화
                    ss.notice_last_school = None
                    ss.notice_last_open_date = None

                    # ✅ 여기서 바로 메시지
                    st.success("학교가 선택되었습니다.")
<<<<<<< HEAD
<<<<<<< HEAD
=======
                    ss.run_result = None
                    ss.run_logs = []

    # 자동완성 리스트(참고용)
    if ss.school_keyword.strip():
        try:
            suggestions = search_schools_in_db(work_root, ss.school_keyword, limit=15)
        except Exception:
            suggestions = []
        if suggestions:
            st.caption("DB 검색 결과")
            st.write(", ".join(suggestions[:15]))
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
=======
            
>>>>>>> 0f1c56e (20260227)

st.divider()

# ============================================================
# 3) 입력 파일 미리보기
# ============================================================
st.header("입력 파일 미리보기")
if not ss.school_ok:
    box_warn("학교를 선택해 주세요.")
else:
    work_root = Path(ss.work_root).resolve()

    if not ss.school_folder_name:
        box_warn("선택된 학교 폴더 정보를 찾을 수 없습니다. 학교를 다시 선택해 주세요.")
    else:
        school_dir = work_root / ss.school_folder_name
        try:
            files = [
                p.name
                for p in school_dir.iterdir()
                if p.is_file() and not p.name.startswith("~$")
            ]
        except FileNotFoundError:
            files = []
            st.error("학교 폴더를 찾을 수 없습니다. 작업 폴더 구조를 다시 확인해 주세요.")

        st.caption(f"총 {len(files)}개의 파일이 존재합니다.")
        if files:
            for fn in files:
                st.write(fn)
        else:
            box_warn("학교 폴더 안에 파일이 없습니다.")

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)
st.divider()

# ============================================================
# 4) 스캔
# ============================================================
st.header("스캔")

if not ss.school_ok:
    box_warn("학교를 선택해 주세요.")
else:
    # 4-1) 스캔 버튼
    if st.button("파일 내용 스캔", use_container_width=True, key="btn_scan"):
<<<<<<< HEAD
=======
    # 스캔 버튼
    if st.button("파일 내용 스캔", use_container_width=True):
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
        try:
            ss.scan = scan_pipeline(
                work_root=Path(ss.work_root),
                school_name=ss.school_selected,  # DB 기준 이름
                school_start_date=ss.open_date,          # 개학일 인자 이름 통일
                work_date=ss.work_date,                  # 작업일
                roster_basis_date=ss.roster_basis_date,  # 명부 기준일(있으면 우선)
            )
            ss.scan_ok = bool(ss.scan.ok)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)

            # 새 스캔마다 레이아웃 기본값 + 카드 체크박스 초기화
            if ss.scan_ok:
                init_default_layout_from_scan(ss)
                for kind in ["freshmen", "transfer", "withdraw", "teacher"]:
                    key = f"layout_ok_{kind}"
                    if key in st.session_state:
                        del st.session_state[key]
            else:
                ss.layout_overrides = {}
        except Exception as e:
            ss.scan = None
            ss.scan_ok = False
            ss.layout_overrides = {}
            st.error(str(e))

    # 4-2) 스캔 결과 + 로그
    if ss.scan is None:
        st.info("파일 내용 스캔을 실행해 주세요.")
    else:
        scan = ss.scan

        # 로그에서 ERROR / WARN 정리 (명부 기준일은 로그에서 안 뽑고 ScanResult에서 바로 씀)
        first_error = None
        other_warns: list[str] = []

        for line in scan.logs or []:
            level, msg = split_log_level(line)

            if level == "ERROR" and first_error is None:
                first_error = msg
            elif level == "WARN":
                other_warns.append(msg)

        # --- ERROR / 일반 WARN 표시 ---
        if not scan.ok:
            if first_error:
                st.error(first_error)
            else:
                st.error(
                    "입력 파일의 형식을 확인해 주세요. "
                    "(신입생 명단, 시트명, 학생명부 파일 여부 등)"
                )
        else:
            # 스캔은 통과했지만 나머지 WARN만 노란 박스로 표시
            for msg in other_warns:
                st.warning(msg)

        # 원본 로그는 그대로 노출
        with st.expander("스캔 로그", expanded=False):
            st.code("\n".join(scan.logs or []), language="text")

        st.markdown("<br>", unsafe_allow_html=True)

        # 4-3) 명부 기준일 + 파일별 데이터 시작 위치 카드 UI
        if scan.ok:
            # ScanResult에 저장된 명부 기준일/필요여부 사용
            need_roster = getattr(scan, "need_roster", False)
            basis_from_scan = getattr(scan, "roster_basis_date", None)
            work_date_val = ss.work_date

            if need_roster and basis_from_scan is not None:
                # 기본값: 스캔 기준일
                if ss.roster_basis_date is None:
                    ss.roster_basis_date = basis_from_scan

                if ss.roster_basis_draft is None:
                    ss.roster_basis_draft = ss.roster_basis_date

                # 기준일 == 작업일인 경우: 그냥 정보만 보여주고 입력 UI는 숨김
                if basis_from_scan == work_date_val:
                    st.caption(
                        f"명부 기준일: {basis_from_scan.isoformat()} "
                        "(학생명부 마지막 수정일과 작업일이 같습니다.)"
                    )
                else:
                    # 자동 감지 기준일 / 작업일
                    st.caption(
                        f"자동 감지된 기준일: {basis_from_scan.isoformat()} "
                        "(학생명부 마지막 수정일)"
                    )
                    st.caption(f"작업일: {work_date_val.isoformat()}")

                    st.warning(
                        "감지된 기준일이 작업일과 다릅니다.\n"
                        "학생명부를 다운받을 때 설정한 기준일을 입력해 주세요."
                    )

                    col_b1, col_b2 = st.columns([4, 1])

                    with col_b1:
                        # 사용자가 만지는 건 draft 값
                        ss.roster_basis_draft = st.date_input(
                            label="명부 기준일",
                            value=ss.roster_basis_draft,
                            key="roster_basis_date_input",
                        )

                    with col_b2:
                        # 라벨 높이만큼 공백 넣어서 수평 맞추기
                        st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
                        apply_clicked_basis = st.button(
                            "적용", use_container_width=True, key="btn_roster_basis_apply"
                        )

                    if apply_clicked_basis:
                        # 성공 메시지 없이 값만 갱신
                        ss.roster_basis_date = ss.roster_basis_draft
            # 데이터 시작 UI 카드
            layout = ss.layout_overrides or {}

            # 실제로 존재하는 파일 종류만 추려서 순서 리스트 생성
            file_kinds: List[str] = [
                k for k in ["freshmen", "transfer", "withdraw", "teacher"]
                if getattr(scan, f"{k}_file", None)
            ]

            for idx, kind in enumerate(file_kinds):
                file_path: Optional[Path] = getattr(scan, f"{kind}_file", None)
                cfg = layout.get(kind, {})
                header_row = int(cfg.get("header_row", 1))
                data_start = int(cfg.get("data_start_row", header_row + 1))

                with st.container():
                    st.markdown(f"**{file_path.name}**")

                    st.caption(
                        f"자동 감지된 데이터 시작 행: {data_start}행"
                    )

                    st.caption(
                        "파일을 열어 확인한 뒤, 필요하면 아래에서 실제 명단이 시작하는 위치를 수정해 주세요."
                    )

                    col_num, col_chk = st.columns([1, 2])
                    with col_num:
                        new_data_start = st.number_input(
                            label="",
                            min_value=1,
                            max_value=9999,
                            value=data_start,
                            step=1,
                            key=f"data_start_{kind}",
                            label_visibility="collapsed",
                        )
                    with col_chk:
                        st.checkbox(
                            "시작 행 위치를 확인했습니다.",
                            key=f"layout_ok_{kind}",
                        )

                layout.setdefault(kind, {})
                layout[kind]["header_row"] = header_row
                layout[kind]["data_start_row"] = int(new_data_start)

                if idx < len(file_kinds) - 1:
                    st.markdown("---")

            ss.layout_overrides = layout

st.divider()

# ============================================================
# 5) 실행
# ============================================================
st.header("실행")

if not ss.school_ok:
    box_warn("학교를 선택해 주세요.")
else:
    # 어떤 파일 종류가 실제로 있는지 기준으로 필수 체크 목록 구성
    required_kinds: List[str] = []
    if ss.scan and ss.scan.ok:
        for kind in ["freshmen", "transfer", "withdraw", "teacher"]:
            if getattr(ss.scan, f"{kind}_file", None):
                required_kinds.append(kind)

    # 카드별 체크박스 상태 확인
    all_confirmed = bool(required_kinds) and all(
        st.session_state.get(f"layout_ok_{k}", False) for k in required_kinds
    )

    # 상태 메시지
    if not ss.scan_ok:
        st.info("먼저 [스캔]을 통과해야 실행할 수 있습니다.")
        run_disabled = True
    else:
        run_disabled = not all_confirmed
        if run_disabled:
            st.info("각 파일의 시작 행 위치를 모두 확인해 주세요.")

    # 스캔 OK + 카드 전부 확인된 경우에만 실행 가능
    if st.button("작업 실행", use_container_width=True, disabled=run_disabled, key="btn_run"):
<<<<<<< HEAD
=======
        except Exception as e:
            ss.scan = None
            ss.scan_ok = False
            st.error(str(e))

st.divider()

# ============================================================
# 4) 스캔 결과
# ============================================================
st.header("스캔 결과")
if ss.scan is None:
    st.info("파일 내용 스캔을 실행해 주세요.")
else:
    if ss.scan.ok:
        st.success("형식이 유효합니다. 실행 가능합니다.")
    else:
        st.error("입력 파일의 형식을 확인해 주세요. (신입생 명단 데이터가 '성별'부터 시작하는지 확인 / 시트명 / 학생명부 필요 여부 등)")
    with st.expander("스캔 로그", expanded=False):
        st.code("\n".join(ss.scan.logs), language="text")

    # 실행 버튼 영역
    st.subheader("실행")
    if st.button("작업 실행", use_container_width=True, disabled=not ss.scan_ok):
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
        res = run_pipeline(
            work_root=Path(ss.work_root),
<<<<<<< HEAD
            school_name=ss.school_selected,
            open_date=ss.open_date,
<<<<<<< HEAD
<<<<<<< HEAD
            layout_overrides=ss.layout_overrides,
=======
>>>>>>> f3aadff (Initial commit)
=======
            layout_overrides=ss.layout_overrides,
>>>>>>> d9c3029 (Add files via upload)
=======
            school_name=ss.school_selected,  # DB 기준 이름
            school_start_date=ss.open_date,          # 개학일 인자 이름 통일
            work_date=ss.work_date,                  # 작업일
            layout_overrides=ss.layout_overrides,
            roster_basis_date=ss.roster_basis_date,  # 명부 기준일(없으면 내부에서 작업일 fallback)
>>>>>>> 0f1c56e (20260227)
        )
        ss.run_result = res
        ss.run_logs = res.logs

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)
        if ss.run_result is not None:
            res = ss.run_result

            # 1) 실행 로그 – 버튼 바로 아래
            with st.expander("실행 로그", expanded=False):
                st.code("\n".join(ss.run_logs or []), language="text")

            # 2) 상태 박스 / 요약
            if res.ok:
                # 처리 건수 읽기 (없으면 0으로)
                ti_done = getattr(res, "transfer_in_done", 0)
                ti_hold = getattr(res, "transfer_in_hold", 0)
                to_done = getattr(res, "transfer_out_done", 0)
                to_hold = getattr(res, "transfer_out_hold", 0)
                to_auto_skip = getattr(res, "transfer_out_auto_skip", 0)

                lines = []

                # 전입 파일 있었을 때
                if ss.scan and getattr(ss.scan, "transfer_file", None):
                    lines.append(
                        f"전입생: 완료 {ti_done}명 / 보류 {ti_hold}명"
                    )

                # 전출 파일 있었을 때
                if ss.scan and getattr(ss.scan, "withdraw_file", None):
                    # 일반 보류 = 전체 보류 - 자동 제외
                    withdraw_hold_manual = max(to_hold - to_auto_skip, 0)

                    line = (
                        f"전출생: 퇴원 {to_done}명 "
                        f"/ 보류 {withdraw_hold_manual}명"
                    )

                    if to_auto_skip > 0:
                        line += f" (자동 제외 {to_auto_skip}명)"

                    lines.append(line)

                # 실제로 보류(수동 확인)가 하나라도 있을 때만 경고
                if (ti_hold > 0 or (to_hold - to_auto_skip) > 0) and lines:
                    msg = "보류 건이 있습니다. 생성된 파일의 보류 시트를 확인해 주세요.\n\n"
                    msg += "\n".join(lines)
                    st.warning(msg)
                else:
                    # 보류 없을 때만 success 박스 + 요약 텍스트
                    st.success("작업이 완료되었습니다.")
            else:
                # 에러 났을 때
                st.error("실행 중 오류가 발생했습니다. 로그를 확인해 주세요.")

            st.write("")  # 상태와 아래 블록 사이 간격

            # 3) 생성 파일
            if res.outputs:
                st.markdown("**생성 파일**")
                for p in res.outputs:
                    st.write(p.name)

                st.write("")

                # 4) 저장 위치 – 실제 작업 출력 폴더 (work_root / 학교 / 작업)
                out_dir = res.outputs[0].parent

<<<<<<< HEAD
            out_dir = res.outputs[0].parent

            st.text_input(
                label="",
                value=str(out_dir),
                label_visibility="collapsed",
                key="run_result_outdir",
            )
<<<<<<< HEAD
=======
    # 실행 결과
    st.divider()
    st.header("실행 결과")

    if ss.run_result is None:
        st.info("실행을 진행해 주세요.")
    else:
        if ss.run_result.ok:
            st.success("[작업] 폴더 안에 파일이 생성되었습니다.")
        else:
            st.error("실행 중 오류가 발생했습니다. 로그를 확인하세요.")

        with st.expander("실행 로그", expanded=False):
            st.code("\n".join(ss.run_logs or []), language="text")

        if ss.run_result.outputs:
            for p in ss.run_result.outputs:
                st.write(f"- {p.name}")
            st.write(f"저장 위치: {(Path(ss.work_root).resolve() / ss.school_selected / '작업')}")
        else:
            st.write("산출물 경로를 확인할 수 없습니다.")
>>>>>>> f3aadff (Initial commit)
=======
>>>>>>> d9c3029 (Add files via upload)
=======
                st.text_input(
                    label="",
                    value=str(out_dir),
                    label_visibility="collapsed",
                    key="run_result_outdir",
                )
>>>>>>> 0f1c56e (20260227)

st.divider()

# ============================================================
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9c3029 (Add files via upload)
# 6) 안내문
# ============================================================
st.header("안내문")
st.caption("안내문 양식의 학교명·학년도·개학일·도메인이 자동으로 치환됩니다.")

if not ss.school_ok:
    box_warn("학교를 먼저 선택하면, 안내문에 학교명/학년도/도메인이 자동으로 치환됩니다.")
else:
    work_root = Path(ss.work_root).resolve()
    dirs = get_project_dirs(work_root)
    db_dir = dirs["DB"]

    # 도메인 조회
    domain: Optional[str] = None
    domain_error_msg: Optional[str] = None
    try:
        domain = get_school_domain_from_db(db_dir, ss.school_selected)
        if not domain and ss.school_selected:
            domain_error_msg = domain_missing_message(ss.school_selected)
    except Exception:
        if ss.school_selected:
            domain_error_msg = domain_missing_message(ss.school_selected)

    # txt 템플릿 로드 (resources/notices/*.txt)
    templates: dict[str, str] = load_notice_templates(work_root)

    if not templates:
        st.error("안내문 텍스트 템플릿을 찾지 못했습니다. resources/notices 폴더에 txt 파일을 넣어주세요.")
    else:
        if domain_error_msg:
            st.error(domain_error_msg)

        # 🔹 방금 학교를 선택한 경우: 성공 메시지 + 안내문 위젯 강제 초기화 플래그
        notice_refresh = False
        if ss.get("school_just_selected", False):
            st.success("학교가 선택되었습니다.")
            notice_refresh = True
            ss.school_just_selected = False

        open_date_val = ss.open_date
        year = open_date_val.year
        prev_year = year - 1
        month = open_date_val.month
        day = open_date_val.day

        # {domain}에는 서브도메인만 들어가야 하는 템플릿 대응
        domain_for_format = ""
        if domain:
            # 맨 끝에 .readinggate.com 붙어 있으면 떼고 전달
            domain_for_format = re.sub(r"\.readinggate\.com$", "", domain.strip())

        # 안내문 제목 목록: 폴더 안 txt 파일명을 그대로 사용 (이름순)
        ordered_keys: List[str] = sorted(templates.keys())

        current_school = ss.school_selected
        current_open_date = ss.open_date

        for key in ordered_keys:
            raw_template = templates.get(key)
            height = 200

            if not raw_template:
                continue

            # 1) 템플릿 치환
            try:
                filled = raw_template.format(
                    school_name=current_school,
                    year=year,
                    prev_year=prev_year,
                    month=month,
                    day=day,
                    domain=domain_for_format,
                )
            except KeyError as e:
                with st.expander(key, expanded=False):
                    st.error(f"템플릿 치환 키 오류: {e}")
                continue

            # 2) 도메인 없을 때 readinggate.com 줄 교체
            if not domain_for_format:
                lines = filled.splitlines()
                new_lines = []
                err_line = domain_missing_message(current_school)
                for line in lines:
                    if "readinggate.com" in line:
                        new_lines.append(err_line)
                    else:
                        new_lines.append(line)
                filled = "\n".join(new_lines)

            # 3) 위젯 state 키
            state_key = f"notice_{key}"

            # 4) 학교 / 개학일이 바뀌었거나, 처음 렌더링이면 state를 새 텍스트로 덮어쓰기
            need_reset = False
            if state_key not in st.session_state:
                need_reset = True
            else:
                if (
                    ss.notice_last_school != current_school
                    or ss.notice_last_open_date != current_open_date
                ):
                    need_reset = True

            if need_reset:
                st.session_state[state_key] = filled

            # 5) 실제 위젯: value 인자 없이, key만 사용
            with st.expander(key, expanded=False):
                st.text_area(
                    label="",
                    key=state_key,
                    height=height,
                    label_visibility="collapsed",
<<<<<<< HEAD
                    key=f"notice_{key}",
<<<<<<< HEAD
                )
=======
# 5) 학교 안내
# ============================================================
st.header("학교 안내")
st.caption("아래 안내문은 내부 고정 템플릿이며, 학교명/도메인만 자동 치환됩니다. (작업 종류와 무관하게 전부 출력)")

if not ss.school_ok:
    box_warn("학교를 먼저 선택하면, 안내문에 학교명/도메인이 자동으로 치환됩니다.")
else:
    work_root = Path(ss.work_root).resolve()
    db_dir = work_root / "DB"
    domain = get_school_domain_from_db(db_dir, ss.school_selected)

    for key in NOTICE_ORDER:
        template = NOTICE_TEMPLATES.get(key, "")
        text = _render_notice_text(template, ss.school_selected, domain)
        with st.expander(f"★ {key}", expanded=False):
            st.text_area("내용", value=text, height=360)
>>>>>>> f3aadff (Initial commit)
=======
                )
>>>>>>> d9c3029 (Add files via upload)
=======
                )

        # 6) 이번 렌더 기준값 저장
        ss.notice_last_school = current_school
        ss.notice_last_open_date = current_open_date
>>>>>>> 0f1c56e (20260227)
