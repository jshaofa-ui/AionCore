"""Re-extract archived originals while preserving notes and provenance."""
import fcntl, hashlib, json
from pathlib import Path
from ingest import extract, save

def reprocess(vault):
    base=Path(vault)/'knowledge/10 - 公司资料库'
    with (base/'.ingest.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        ledger=base/'.ledger.json'
        state=json.loads(ledger.read_text())
        recovered=0
        for digest,meta in state.items():
            if meta.get('text_path'):continue
            original=base/meta['original_path']
            if hashlib.sha256(original.read_bytes()).hexdigest()!=digest:
                raise ValueError('Original checksum mismatch')
            text,status=extract(original)
            if not text.strip():continue
            textfile=base/'正文'/(digest+'.txt')
            save(textfile,text.encode('utf-8'))
            note=base/meta['note_path']
            body=note.read_text()
            body=body.replace('extraction_status: '+meta['extraction_status'], 'extraction_status: '+status,1)
            body+='\n\n## 正文补充提取\n\n'+('OCR 结果待人工核对。\n\n' if status.startswith('ocr_') else '')+text[:160000]+'\n'
            save(note,body.encode('utf-8'))
            old=meta['extraction_status']
            meta.update(text_path=str(textfile.relative_to(base)),extraction_status=status)
            save(base/meta['source_record'],json.dumps(meta,ensure_ascii=False,indent=2).encode())
            index=base/'公司资料库.md'
            if index.exists():
                content=index.read_text()
                content=content.replace('|'+meta['original_name'].replace(']','_')+']] · '+old, '|'+meta['original_name'].replace(']','_')+']] · '+status)
                save(index,content.encode())
            recovered+=1
        save(ledger,json.dumps(state,ensure_ascii=False,indent=2).encode())
        print(json.dumps({'recovered':recovered,'documents':len(state)}))

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--vault',required=True)
    reprocess(p.parse_args().vault)
