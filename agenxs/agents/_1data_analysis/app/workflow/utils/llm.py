from __future__ import annotations

import os
import google.generativeai as genai


def suggest_cleaning_with_llm(profile, quality_issues) -> str:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY in environment (.env)")

    genai.configure(api_key=api_key)

    # model name can be gemini-1.5-flash or gemini-2.0-flash depending on your key access
    model = genai.GenerativeModel("gemini-2.5-flash")

    prompt = f"""
        You are a data cleaning expert.
        
        Return ONLY valid JSON (no markdown, no ``` fences, no extra text).
        The JSON must match this schema exactly:
        
        {{
          "rename_columns": {{}},
          "drop_columns": [],
          "cast_columns": {{}},
          "trim_strings": [],
          "fill_missing": {{}},
          "deduplicate": null,
          "date_formats": {{}}
        }}
        
        Where:
        - cast_columns values must be one of: "int","float","str","date","datetime","bool"
        - fill_missing values must be objects like: {{"strategy":"mean|median|mode|constant","value": <only if constant>}}
        - deduplicate is either null OR {{"subset":[...],"keep":"first|last"}}
        
        Dataset profile:
        {profile}
        
        Quality issues:
        {quality_issues}
        
        Now output JSON only:
        """.strip()

    resp = model.generate_content(prompt)
    return resp.text