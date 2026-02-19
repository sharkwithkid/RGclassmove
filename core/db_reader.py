# core/db_reader.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Optional

from pyxlsb import open_workbook as open_xlsb_workbook

from core.utils import normalize_text, text_contains, text_eq


# =========================
# 설정값(필요하면 여기만 수정)
# =========================
DB_KEYWORD = "학교전체명단"
PREFERRED_SHEET = "학교명단"
SCHOOL_HEADER = "학교명"

COL_E_IDX = 4         # 0-based index (A=0 ... E=4)
MAX_SCAN_ROWS = 5000  # 안전장치

# 값 제외(정규화 비교로 처리)
EXCLUDE_VALUES = {
    SCHOOL_HEADER,
    "-",
    "—",
    "–",
}


@dataclass(frozen=True)
class DBMeta:
    selected_file: str
    keyword_matched: bool
    sheet_used: str
    header_blocks: int
    school_count: int


def _cell_to_str(cell) -> Optional[str]:
    """pyxlsb cell -> stripped str or None"""
    if cell is None:
        return None
    v = getattr(cell, "v", None)
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _iter_xlsb_paths(db_root: Path) -> List[Path]:
    """DB 폴더에서 .xlsb 파일을 모두 찾되, 엑셀 임시 잠금파일(~$) 제외"""
    if not db_root.exists():
        raise FileNotFoundError(f"DB 폴더가 없습니다: {db_root}")

    files = [p for p in db_root.rglob("*.xlsb") if p.is_file()]
    files = [p for p in files if not p.name.startswith("~$")]
    return files


def pick_db_xlsb(db_root: Path, keyword: str = DB_KEYWORD) -> Tuple[Path, bool]:
    """
    DB 폴더 내 .xlsb 파일 선택 규칙
    1) 파일명에 keyword(정규화 포함) 매칭되는 파일 우선
    2) 여러 개면 수정일 최신
    3) 없으면 전체 xlsb 중 수정일 최신 + keyword_matched=False
    """
    files = _iter_xlsb_paths(db_root)
    if not files:
        raise FileNotFoundError(f"DB 폴더에서 .xlsb 파일을 찾지 못했습니다: {db_root}")

    cands = [p for p in files if text_contains(p.name, keyword)]
    keyword_matched = len(cands) > 0

    use = cands if cands else files
    use.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return use[0], keyword_matched


def _count_header_blocks(xlsb_path: Path, sheet_name: str) -> int:
    """특정 시트의 E열에서 '학교명' 헤더가 몇 번 나오는지(블록 수)"""
    blocks = 0
    with open_xlsb_workbook(xlsb_path) as wb:
        with wb.get_sheet(sheet_name) as sh:
            for i, row in enumerate(sh.rows()):
                if i >= MAX_SCAN_ROWS:
                    break
                cells = list(row)
                val = _cell_to_str(cells[COL_E_IDX]) if len(cells) > COL_E_IDX else None
                if text_eq(val, SCHOOL_HEADER):
                    blocks += 1
    return blocks


def choose_sheet_for_school_list(xlsb_path: Path) -> str:
    """
    1) '학교명단' 시트가 있으면 우선
    2) 없으면: E열에 '학교명' 헤더 블록이 가장 많은 시트 선택
    """
    with open_xlsb_workbook(xlsb_path) as wb:
        sheetnames = list(wb.sheets)

    if PREFERRED_SHEET in sheetnames:
        return PREFERRED_SHEET

    best_sheet = sheetnames[0]
    best_blocks = -1

    for sh in sheetnames:
        try:
            blocks = _count_header_blocks(xlsb_path, sh)
        except Exception:
            continue
        if blocks > best_blocks:
            best_blocks = blocks
            best_sheet = sh

    return best_sheet


def load_school_names(db_root: Path, keyword: str = DB_KEYWORD) -> Tuple[List[str], DBMeta]:
    """
    DB에서 학교명 리스트 로드

    - DB 파일 선택: 키워드 우선 + 최신 수정일
    - 시트 선택: 학교명단 우선, 없으면 헤더 블록 최다 시트
    - 추출: E열에서 '학교명' 헤더 아래의 '문자열 값'을 모두 수집 (여러 블록 지원)
    - 중복 제거(순서 유지)
    """
    xlsb_path, matched = pick_db_xlsb(db_root, keyword=keyword)
    sheet = choose_sheet_for_school_list(xlsb_path)

    schools: List[str] = []
    seen = set()

    header_blocks = 0
    in_block = False

    # 제외값은 정규화해둔 set으로 비교
    exclude_norm = {normalize_text(x) for x in EXCLUDE_VALUES}

    with open_xlsb_workbook(xlsb_path) as wb:
        with wb.get_sheet(sheet) as sh:
            for i, row in enumerate(sh.rows()):
                if i >= MAX_SCAN_ROWS:
                    break

                cells = list(row)
                val = _cell_to_str(cells[COL_E_IDX]) if len(cells) > COL_E_IDX else None

                # '학교명' 헤더 감지 → 블록 시작(여러 번 가능)
                if text_eq(val, SCHOOL_HEADER):
                    in_block = True
                    header_blocks += 1
                    continue

                if not in_block:
                    continue

                if val is None:
                    continue

                # 제외값/구분선 제거
                vnorm = normalize_text(val)
                if not vnorm:
                    continue
                if vnorm in exclude_norm:
                    continue
                if all(ch in "-—–" for ch in val.strip()):
                    continue

                # 학교명 수집(순서 유지 중복 제거)
                if val not in seen:
                    seen.add(val)
                    schools.append(val)

    meta = DBMeta(
        selected_file=str(xlsb_path),
        keyword_matched=matched,
        sheet_used=sheet,
        header_blocks=header_blocks,
        school_count=len(schools),
    )
    return schools, meta
