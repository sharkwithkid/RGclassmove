# app.py (Streamlit)
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional, List
import re

import streamlit as st

from core.pipeline import (
    scan_work_root,
    scan_pipeline,
    run_pipeline,
    search_schools_in_db,
    get_school_domain_from_db,
    domain_missing_message,
    detect_input_layout,  # 🔹 자동 레이아웃 감지
)

st.set_page_config(page_title="리딩게이트 반이동 자동화", layout="wide")

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


# -------------------------
# 안내문 제목 순서 (파일명과 동일)
# -------------------------
NOTICE_ORDER = [
    "신규등록 - 메일",
    "신규등록 - 문자",
    "교직원만 등록 - 메일",
    "반이동 - 메일",
    "반이동 - 메일 (신입생, 교직원 등록 & 반이동)",
    "반이동 - 문자",
    "2-6학년 명단 보내 온 경우 - 메일",
    "2-6학년 반편성 자료 재요청 - 문자",
]


@st.cache_data
def get_all_school_names(work_root_str: str) -> List[str]:
    if not work_root_str:
        return []

    root = Path(work_root_str)
    names: set[str] = set()

    try:
        for kw in ("초", "중", "고"):
            for s in search_schools_in_db(root, kw, limit=10000):
                names.add(s)
    except Exception:
        return []

    return sorted(names)


# -------------------------
# session init
# -------------------------
ss = st.session_state
ss.setdefault("work_root", "")
ss.setdefault("path_scan", None)
ss.setdefault("config_applied", False)
ss.setdefault("open_date", date.today())

ss.setdefault("school_selected", "")
ss.setdefault("school_ok", False)

ss.setdefault("scan", None)
ss.setdefault("scan_ok", False)

# 레이아웃(데이터 시작 행) 상태
# kind -> {"header_row": int, "data_start_row": int, "example_rows": List[int]}
ss.setdefault("layout_overrides", {})

ss.setdefault("run_result", None)
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
        if not ss.work_root.strip():
            ss.path_scan = {"ok": False, "errors": ["[오류] 작업 폴더 경로가 비어 있습니다."]}
        else:
            ss.path_scan = scan_work_root(Path(ss.work_root))
        # 설정/상태 초기화
        ss.config_applied = False
        ss.school_selected = ""
        ss.school_ok = False
        ss.scan = None
        ss.scan_ok = False
        ss.layout_overrides = {}
        ss.run_result = None
        ss.run_logs = []

if ss.path_scan is not None:
    if not ss.path_scan.get("db_ok", False):
        box_errors(ss.path_scan.get("errors_db", ["[DB] 폴더/파일을 확인해주세요."]))
    else:
        dbf = ss.path_scan.get("db_file")
        st.write(f"[DB] {dbf.name if dbf else '-'}")

    if not ss.path_scan.get("format_ok", False):
        box_errors(ss.path_scan.get("errors_format", ["[양식] 폴더/파일을 확인해주세요."]))
    else:
        reg_t = ss.path_scan.get("register_template")
        notice_t = ss.path_scan.get("notice_template")
        st.write(f"[양식] {reg_t.name if reg_t else '-'}")
        st.write(f"[양식] {notice_t.name if notice_t else '-'}")

st.subheader("개학일")
col_d1, col_d2 = st.columns([4, 1])
with col_d1:
    ss.open_date = st.date_input(
        label="",
        value=ss.open_date,
        label_visibility="collapsed",
        key="open_date_input",
    )
with col_d2:
    if st.button("적용", use_container_width=True, key="btn_apply_open_date"):
        # 값만 반영
        pass

st.markdown("")

can_apply = ss.path_scan is not None and ss.path_scan.get("ok", False)
if st.button("설정 저장", use_container_width=True, disabled=not can_apply, key="btn_config_save"):
    ss.config_applied = True
    box_success("설정이 저장되었습니다.")

st.divider()

# ============================================================
# 2) 학교 선택
# ============================================================
st.header("학교 선택")
st.caption("작업 폴더 안에 있는 학교 폴더 이름은 DB에 등록된 학교명과 동일해야 합니다.")

if not ss.config_applied:
    box_warn("먼저 [기본 설정]에서 경로를 적용하고 설정을 저장해 주세요.")
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
                box_warn("학교를 선택해 주세요.")
            else:
                name = selected_name.strip()
                school_dir = work_root / name

                if not school_dir.exists():
                    ss.school_ok = False
                    ss.school_selected = ""
                    st.error("설정한 작업 폴더 안에 해당 학교 폴더가 없습니다. 폴더를 생성한 후 다시 시도해 주세요.")
                else:
                    ss.school_selected = name
                    ss.school_ok = True
                    ss.scan = None
                    ss.scan_ok = False
                    ss.layout_overrides = {}
                    ss.run_result = None
                    ss.run_logs = []
                    st.success("학교가 선택되었습니다.")

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
        try:
            ss.scan = scan_pipeline(
                work_root=Path(ss.work_root),
                school_name=ss.school_selected,
                open_date=ss.open_date,
            )
            ss.scan_ok = bool(ss.scan.ok)

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
        if not ss.scan.ok:
            first_error = None
            for line in ss.scan.logs or []:
                level, msg = split_log_level(line)
                if level == "ERROR":
                    first_error = msg  # [ERROR] 떼고 메시지만
                    break
            if first_error:
                st.error(first_error)
            else:
                st.error(
                    "입력 파일의 형식을 확인해 주세요. "
                    "(신입생 명단, 시트명, 학생명부 파일 여부 등)"
                )
        else:
            # 스캔은 통과했지만, WARN은 별도로 박스로 띄우기
            warn_msgs = []
            for line in ss.scan.logs or []:
                level, msg = split_log_level(line)
                if level == "WARN":
                    warn_msgs.append(msg)  # [WARN] 떼고 메시지만 사용

            for msg in warn_msgs:
                st.warning(msg)

        with st.expander("스캔 로그", expanded=False):
            # 여기에는 원본 로그 그대로 (대괄호 포함)
            st.code("\n".join(ss.scan.logs), language="text")

        # 로그와 첫 카드 사이 여백
        st.markdown("<br>", unsafe_allow_html=True)

        # 4-3) 파일별 데이터 시작 위치 카드 UI
        if ss.scan.ok:
            layout = ss.layout_overrides or {}

            # 실제로 존재하는 파일 종류만 추려서 순서 리스트 생성
            file_kinds: List[str] = [
                k for k in ["freshmen", "transfer", "withdraw", "teacher"]
                if getattr(ss.scan, f"{k}_file", None)
            ]

            for idx, kind in enumerate(file_kinds):
                file_path: Optional[Path] = getattr(ss.scan, f"{kind}_file", None)
                cfg = layout.get(kind, {})
                header_row = int(cfg.get("header_row", 1))
                data_start = int(cfg.get("data_start_row", header_row + 1))

                with st.container():
                    st.markdown(f"**{file_path.name}**")

                    # 헤더 문구 제거 + (자동 감지) 유지
                    st.write(
                        f"예시를 제외한 실제 명단 시작 위치: {data_start}행 (자동 감지)"
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

                # 마지막 카드가 아닐 때만 구분선
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
            st.info("각 파일 카드에서 시작 행을 모두 확인해 주세요.")
    # else: 메시지 없음 → 버튼만 노출

    # 스캔 OK + 카드 전부 확인된 경우에만 실행 가능
    if st.button("작업 실행", use_container_width=True, disabled=run_disabled, key="btn_run"):
        res = run_pipeline(
            work_root=Path(ss.work_root),
            school_name=ss.school_selected,
            open_date=ss.open_date,
            layout_overrides=ss.layout_overrides,
        )
        ss.run_result = res
        ss.run_logs = res.logs

        if ss.run_result is not None:
            res = ss.run_result

            # 1) 실행 로그 – 버튼 바로 아래
            with st.expander("실행 로그", expanded=False):
                st.code("\n".join(ss.run_logs or []), language="text")

            # 2) 상태 박스 / 요약
            if res.ok:
                # 처리 건수 읽기
                ti_done = getattr(res, "transfer_in_done", 0)
                ti_hold = getattr(res, "transfer_in_hold", 0)
                to_done = getattr(res, "transfer_out_done", 0)
                to_hold = getattr(res, "transfer_out_hold", 0)

                total_hold = ti_hold + to_hold

                if total_hold > 0:
                    # 보류 있을 때: success 박스 없음, 노란 경고 박스 하나만
                    msg_lines = [
                        "보류 건이 있습니다. 생성된 파일의 보류 시트를 확인해 주세요.",
                        "",
                        f"전입생: 완료 {ti_done}명 / 보류 {ti_hold}명  ",
                        f"전출생: 퇴원 {to_done}명 / 보류 {to_hold}명",
                    ]

                    st.warning("\n".join(msg_lines))
                else:
                    # 보류 없을 때만 success 박스 + 요약 텍스트
                    st.success("작업이 완료되었습니다.")
            else:
                # 에러 났을 때
                st.error("실행 중 오류가 발생했습니다. 로그를 확인해 주세요.")

            st.write("")  # 상태와 아래 블록 사이 간격

            # 3) 생성 파일
        if res.outputs:
            # 생성 파일 – 입력 파일 미리보기와 동일한 느낌
            st.markdown("**생성 파일**")
            for p in res.outputs:
                st.write(p.name)

            st.write("")

            # 4) 저장 위치 – 실제 작업 출력 폴더 (work_root / 학교 / 작업)
            st.markdown("**저장 위치**")

            out_dir = res.outputs[0].parent

            st.text_input(
                label="",
                value=str(out_dir),
                label_visibility="collapsed",
                key="run_result_outdir",
            )

st.divider()

# ============================================================
# 6) 안내문
# ============================================================
st.header("안내문")
st.caption("양식/안내문 텍스트를 사용하며, 학교명·학년도·개학일·도메인이 자동으로 치환됩니다.")

if not ss.school_ok:
    box_warn("학교를 먼저 선택하면, 안내문에 학교명/학년도/도메인이 자동으로 치환됩니다.")
else:
    work_root = Path(ss.work_root).resolve()
    db_dir = work_root / "DB"

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

    # txt 템플릿 로드 (양식/안내문/*.txt)
    notice_dir = work_root / "양식" / "안내문"
    templates: dict[str, str] = {}

    if notice_dir.exists():
        for p in notice_dir.glob("*.txt"):
            if not p.is_file():
                continue
            try:
                text = p.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                text = p.read_text(encoding="utf-8-sig")
            templates[p.stem.strip()] = text.strip()

    if not templates:
        st.error("안내문 텍스트 템플릿을 찾지 못했습니다. [양식]/안내문 폴더에 txt 파일을 넣어주세요.")
    else:
        if domain_error_msg:
            st.error(domain_error_msg)

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

        for key in NOTICE_ORDER:
            raw_template = templates.get(key)
            height = 200

            if not raw_template:
                with st.expander(key, expanded=False):
                    st.error(f"{key}.txt 템플릿을 찾지 못했습니다.")
                continue

            try:
                filled = raw_template.format(
                    school_name=ss.school_selected,
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

            # 도메인 없을 때는 readinggate.com 줄만 교체
            if not domain_for_format:
                lines = filled.splitlines()
                new_lines = []
                err_line = domain_missing_message(ss.school_selected)
                for line in lines:
                    if "readinggate.com" in line:
                        new_lines.append(err_line)
                    else:
                        new_lines.append(line)
                filled = "\n".join(new_lines)

            with st.expander(key, expanded=False):
                st.text_area(
                    label="",
                    value=filled,
                    height=height,
                    label_visibility="collapsed",
                    key=f"notice_{key}",
                )