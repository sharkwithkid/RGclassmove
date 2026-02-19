# app.py (Streamlit)
from __future__ import annotations

from datetime import date
import re
from pathlib import Path
from typing import Optional, List

import streamlit as st

from core.pipeline import (
    scan_work_root,
    scan_pipeline,
    run_pipeline,
    search_schools_in_db,
    get_school_domain_from_db,
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

# -------------------------
# session init
# -------------------------
ss = st.session_state
ss.setdefault("work_root", "")
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

# -------------------------
# helpers
# -------------------------
def box_errors(msgs: List[str]):
    for m in msgs:
        st.error(m)

def box_success(msg: str):
    st.success(msg)

def box_warn(msg: str):
    st.warning(msg)

def fmt_path(p: Optional[Path]) -> str:
    return "-" if p is None else str(p)

# -------------------------
# Header
# -------------------------
st.title("리딩게이트 반이동 자동화")

# ============================================================
# 1) 기본 설정(최초 1회)
# ============================================================
st.header("기본 설정 (최초 1회)")

st.subheader("작업 폴더")
st.caption("작업 폴더 내부에 [DB] / [양식] 폴더를 생성하여 필요한 파일을 위치시킨 후 진행해 주세요.")

col1, col2 = st.columns([4, 1])
with col1:
    ss.work_root = st.text_input("폴더 경로", value=ss.work_root, placeholder="/Users/.../2026반이동")
with col2:
    if st.button("경로 적용", use_container_width=True):
        if not ss.work_root.strip():
            ss.path_scan = {"ok": False, "errors": ["[오류] 작업 폴더 경로가 비어 있습니다."]}
        else:
            ss.path_scan = scan_work_root(Path(ss.work_root))
        # 경로 바뀌면 이후 단계 초기화
        ss.config_applied = False
        ss.school_selected = ""
        ss.school_ok = False
        ss.scan = None
        ss.scan_ok = False
        ss.run_result = None
        ss.run_logs = []

# 경로 적용 결과 표시
if ss.path_scan is not None:
    # DB 상태
    if not ss.path_scan.get("db_ok", False):
        box_errors(ss.path_scan.get("errors_db", ["[DB] 폴더/파일을 확인해주세요."]))
    else:
        dbf = ss.path_scan.get("db_file")
        st.write(f"[DB] {dbf.name if dbf else '-'}")

    # 양식(templates) 상태
    if not ss.path_scan.get("format_ok", False):
        box_errors(ss.path_scan.get("errors_format", ["[양식] 폴더/파일을 확인해주세요."]))
    else:
        reg_t = ss.path_scan.get("register_template")
        notice_t = ss.path_scan.get("notice_template")
        st.write(f"[양식] {reg_t.name if reg_t else '-'}")
        st.write(f"[양식] {notice_t.name if notice_t else '-'}")


st.subheader("개학일")
ss.open_date = st.date_input("개학일 입력", value=ss.open_date)

# 설정 적용
can_apply = ss.path_scan is not None and ss.path_scan.get("ok", False)
if st.button("설정 적용", use_container_width=True, disabled=not can_apply):
    ss.config_applied = True
    box_success("설정이 적용되었습니다.")

st.divider()

# ============================================================
# 2) 학교 선택
# ============================================================
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
                if not school_dir.exists():
                    ss.school_ok = False
                    ss.school_selected = ""
                    st.error("설정한 작업 폴더 안에 해당 학교 폴더가 없습니다. 폴더를 생성한 후 다시 시도해 주세요.")
                else:
                    # DB 존재 확인은 scan_pipeline에서 한 번 더 검증됨
                    ss.school_selected = name
                    ss.school_ok = True
                    ss.scan = None
                    ss.scan_ok = False
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

st.divider()

# ============================================================
# 3) 입력 파일 미리보기
# ============================================================
st.header("입력 파일 미리보기")
if not ss.school_ok:
    box_warn("학교를 선택해 주세요.")
else:
    school_dir = Path(ss.work_root).resolve() / ss.school_selected
    files = [p.name for p in school_dir.iterdir() if p.is_file() and not p.name.startswith("~$")]
    st.caption(f"총 {len(files)}개의 파일이 존재합니다.")
    if files:
        for fn in files:
            st.write(fn)
    else:
        box_warn("학교 폴더 안에 파일이 없습니다.")

    # 스캔 버튼
    if st.button("파일 내용 스캔", use_container_width=True):
        try:
            ss.scan = scan_pipeline(
                work_root=Path(ss.work_root),
                school_name=ss.school_selected,
                open_date=ss.open_date,
            )
            ss.scan_ok = bool(ss.scan.ok)
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
        res = run_pipeline(
            work_root=Path(ss.work_root),
            school_name=ss.school_selected,
            open_date=ss.open_date,
        )
        ss.run_result = res
        ss.run_logs = res.logs

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

st.divider()

# ============================================================
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
