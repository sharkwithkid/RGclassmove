#!/usr/bin/env python
"""
core/pipeline.py 안의 함수들을 L0~L5 섹션으로 재배열해서
core/pipeline_v2.py 로 저장하는 스크립트.

- 함수 내부 로직은 그대로 유지하고, 의존성에 의거하여 순서만 재배치.
"""

import ast
from pathlib import Path
from typing import Dict, List
from collections import defaultdict

INPUT_PATH = Path("core/pipeline.py")
OUTPUT_PATH = Path("core/pipeline_v2.py")


def read_code(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {path}")
    return path.read_text(encoding="utf-8")


def classify_layer(name: str) -> str:
    """
    함수명을 기준으로 L0~L5 어느 섹션에 넣을지 결정.
    (우리가 앞에서 잡은 분류 기준 + 약간의 휴리스틱)
    """

    # L0: infra / excel utils
    l0_names = {
        "safe_load_workbook",
        "backup_if_exists",
        "clear_sheet_rows",
        "delete_rows_below",
        "find_last_data_row",
        "header_map",
        "move_sheet_after",
        "reset_view_to_a1",
        "ensure_xlsx_only",
        "write_text_cell",
        "clear_format_workbook_from_row",
        "_get",
        "_safe_int",
        "_format_sheet",
    }

    # L1: domain utils (names / headers / examples)
    l1_prefixes = ("normalize_", "detect_")
    l1_names = {
        "english_casefold_key",
        "notice_name_key",
        "dedup_suffix_letters",
        "apply_suffix_for_duplicates",
        "_build_header_slot_map",
        "_cell_is_example_name",
        "_detect_header_row_generic",
        "_find_suffix_candidates_for_grade",
        "_fix_en",
        "_normalize_header_cell",
        "_row_has_example_keyword",
        "_row_is_empty",
        "_strip_korean_suffix_for_notice",
        "_strip_name_suffix",
    }

    # L2: input readers
    l2_names = {
        "read_freshmen_rows",
        "read_teacher_rows",
        "read_transfer_rows",
        "read_withdraw_rows",
        "normalize_withdraw_class",
    }

    # L3: roster / transfer / withdraw core logic
    l3_names = {
        "analyze_roster_once",
        "build_transfer_ids",
        "build_withdraw_outputs",
        "class_sort_key",
        "extract_id_prefix4",
        "group_name_from_class",
        "load_roster_sheet",
        "parse_class_str",
        "parse_grade_class",
        "parse_roster_year_from_filename",
        "_index_class_map",
        "_index_grade_map",
        "_norm_sheetname",
        "_pick_sheet_by_keywords",
    }

    # L4: output writers (등록/안내/퇴원)
    l4_names = {
        "build_notice_file",
        "build_notice_student_sheet",
        "build_notice_teacher_sheet",
        "fill_register",
        "make_register_class_name",
        "render_mail_text",
        "school_kind_from_name",
        "write_transfer_hold_sheet",
        "write_withdraw_to_register",
        "write_student_row",
        "_parse_grade_class_from_register",
    }

    # L5: orchestrator (scan / execute / run)
    l5_names = {
        "get_project_dirs",
        "find_templates",
        "scan_work_root",
        "find_single_input_file",
        "choose_template_register",
        "choose_template_notice",
        "choose_db_xlsb",
        "search_schools_in_db",
        "school_exists_in_db",
        "_normalize_domain",
        "get_school_domain_from_db",
        "load_notice_templates",
        "domain_missing_message",
        "scan_pipeline",
        "_extract_layout",
        "execute_pipeline",
        "run_pipeline",
        "run_pipeline_partial",
    }

    # 1) 명시 매핑 우선
    if name in l0_names:
        return "L0"
    if name in l1_names or name.startswith(l1_prefixes):
        return "L1"
    if name in l2_names or name.startswith("read_"):
        return "L2"
    if name in l3_names:
        return "L3"
    if name in l4_names:
        return "L4"
    if name in l5_names:
        return "L5"

    # 2) 휴리스틱
    # 출력 쪽으로 보이는 이름
    if (
        "notice" in name
        or "register" in name
        or "withdraw" in name
        or "student_row" in name
    ):
        return "L4"

    # 로스터/전입·전출/반/학년 규칙 쪽
    if (
        "roster" in name
        or "transfer" in name
        or "withdraw" in name
        or "class" in name
        or "grade" in name
    ):
        return "L3"

    # 나머지는 orchestrator로 묶는다
    return "L5"


def build_reordered_code(original_code: str) -> str:
    tree = ast.parse(original_code)
    lines = original_code.splitlines(keepends=True)

    # top-level 함수/클래스/기타 구분
    func_nodes: List[ast.FunctionDef] = [
        n for n in tree.body if isinstance(n, ast.FunctionDef)
    ]
    class_nodes: List[ast.ClassDef] = [
        n for n in tree.body if isinstance(n, ast.ClassDef)
    ]

    # 함수가 차지하는 라인 마스크
    n_lines = len(lines)
    mask = [True] * n_lines
    for fn in func_nodes:
        for i in range(fn.lineno - 1, fn.end_lineno):
            mask[i] = False

    # 함수 정의를 제거한 나머지 코드 (import, 상수, 클래스, etc.)
    base_code_without_funcs = "".join(
        line for i, line in enumerate(lines) if mask[i]
    )

    # 레이어별로 함수 이름 모으기 (원래 순서 유지)
    buckets: Dict[str, List[ast.FunctionDef]] = defaultdict(list)
    for fn in func_nodes:
        layer = classify_layer(fn.name)
        buckets[layer].append(fn)

    # source 추출 helper
    def get_src(node: ast.AST) -> str:
        return "".join(lines[node.lineno - 1 : node.end_lineno])

    # 섹션 헤더
    section_titles = {
        "L0": "# ========== L0: infra / excel utils ==========\n\n",
        "L1": "# ========== L1: domain utils (names / headers / examples) ==========\n\n",
        "L2": "# ========== L2: input readers (신입/전입/전출/교사) ==========\n\n",
        "L3": "# ========== L3: roster / transfer / withdraw core logic ==========\n\n",
        "L4": "# ========== L4: output writers (등록/안내/퇴원) ==========\n\n",
        "L5": "# ========== L5: orchestrator (scan / execute / run) ==========\n\n",
    }

    sections_order = ["L0", "L1", "L2", "L3", "L4", "L5"]
    new_sections: List[str] = []

    for layer in sections_order:
        fns = buckets.get(layer, [])
        if not fns:
            continue
        parts: List[str] = [section_titles[layer]]
        for fn in fns:
            src = get_src(fn)
            parts.append(src)
            if not src.endswith("\n"):
                parts.append("\n")
            parts.append("\n")
        new_sections.append("".join(parts))

    new_code = base_code_without_funcs.rstrip() + "\n\n\n" + "\n".join(new_sections)
    return new_code


def main() -> None:
    print(f"[INFO] 입력 파일: {INPUT_PATH}")
    code = read_code(INPUT_PATH)
    new_code = build_reordered_code(code)
    OUTPUT_PATH.write_text(new_code, encoding="utf-8")
    print(f"[OK] 재정렬된 파일을 저장했습니다: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()