#!/usr/bin/env python3
"""Archive received company documents into an existing Obsidian vault; no LLM calls."""
import argparse,datetime,fcntl,hashlib,json,os,re,shutil,subprocess,tempfile,zipfile
from pathlib import Path
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo
MAX_BYTES=50*1024*1024

def save(path,data):
    path.parent.mkdir(parents=True,exist_ok=True)
    fd,name=tempfile.mkstemp(prefix='.ingest-',dir=path.parent)
    try:
        with os.fdopen(fd,'wb')as f:f.write(data);f.flush();os.fsync(f.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name):os.unlink(name)

def extract(path):
    ext=path.suffix.lower()
    if ext in ['.jpg','.jpeg','.png','.tiff','.tif','.heic']:
        helper=Path(__file__).with_name('ocr')
        if not helper.is_file():return '', 'ocr_required'
        try:
            p=subprocess.run([str(helper),str(path)],capture_output=True,timeout=90)
            text=p.stdout.decode('utf-8',errors='replace')
            return (text,'ocr_extracted_review')if p.returncode==0 and text.strip()else('', 'ocr_required')
        except (OSError,subprocess.TimeoutExpired):return '', 'ocr_required'
    if ext=='.xls':
        try:
            import xlrd
        except ImportError:return '', 'xls_extractor_required'
        try:
            book=xlrd.open_workbook(str(path),on_demand=True)
            try:
                parts=[]
                for sheet in book.sheets():
                    parts.append('['+sheet.name+']')
                    for row in sheet.get_rows():
                        parts.append('\t'.join(str(cell.value) for cell in row))
                text='\n'.join(parts)
                return text,'extracted'if any(s.nrows for s in book.sheets())else'extraction_review'
            finally:book.release_resources()
        except Exception:return '', 'xls_review'
    if ext in ['.txt','.md','.csv','.json','.log']:
        raw=path.read_bytes()
        for encoding in ['utf-8-sig','gb18030']:
            try:return raw.decode(encoding),'extracted'
            except UnicodeError:pass
        return '', 'encoding_review'
    if ext in ['.docx','.xlsx','.pptx']:
        try:
            with zipfile.ZipFile(path)as z:
                members=[i for i in z.infolist()if i.filename in ['word/document.xml','xl/sharedStrings.xml']or re.fullmatch(r'(?:xl/worksheets/sheet\d+|ppt/slides/slide\d+)\.xml',i.filename)]
                if sum(i.file_size for i in members)>30*1024*1024:return '', 'extraction_size_review'
                parts=[]
                for member in sorted(members,key=lambda x:x.filename):
                    tree=ET.fromstring(z.read(member))
                    for node in tree.iter():
                        if node.tag.rsplit('}',1)[-1]in ['p','row']:
                            text=' '.join(x.text or ''for x in node.iter()if x.tag.rsplit('}',1)[-1]in ['t','v'])
                            if text.strip():parts.append(text)
                text='\n'.join(parts);return text, 'extracted'if text.strip()else'extraction_review'
        except (zipfile.BadZipFile,ET.ParseError,KeyError):return '', 'extraction_review'
    if ext=='.pdf':
        try:
            import fitz
            with fitz.open(path)as doc:text='\n\n'.join(page.get_text()for page in doc)
            return text,'extracted'if text.strip()else'ocr_required'
        except ImportError:pass
        except Exception:return '', 'pdf_review'
        if shutil.which('pdftotext'):
            p=subprocess.run(['pdftotext','-layout',str(path),'-'],capture_output=True,timeout=90)
            if p.returncode==0:
                text=p.stdout.decode('utf-8',errors='replace');return text,'extracted'if text.strip()else'ocr_required'
        try:
            from pypdf import PdfReader
            text='\n\n'.join(p.extract_text()or''for p in PdfReader(path).pages)
            return text,'extracted'if text.strip()else'ocr_required'
        except ImportError:return '', 'pdf_extractor_required'
        except Exception:return '', 'pdf_review'
    if ext in ['.doc','.rtf']and Path('/usr/bin/textutil').exists():
        p=subprocess.run(['/usr/bin/textutil','-convert','txt','-stdout',str(path)],capture_output=True,timeout=90)
        if p.returncode==0:return p.stdout.decode('utf-8',errors='replace'),'extracted'
    return '', 'format_review'

def ingest(request):
    vault=Path(request['vault']).expanduser().resolve()
    if not (vault/'.obsidian').is_dir():raise ValueError('Target must be an existing Obsidian vault')
    base=vault/'knowledge/10 - 公司资料库';base.mkdir(parents=True,exist_ok=True)
    lock=base/'.ingest.lock'
    with lock.open('a')as fd:
        fcntl.flock(fd,fcntl.LOCK_EX)
        ledger=base/'.ledger.json';state=json.loads(ledger.read_text())if ledger.exists()else{}
        results=[]
        for item in request['files']:
            source=Path(item['path']).resolve()
            if not source.is_file()or source.stat().st_size>MAX_BYTES:raise ValueError('Missing or oversized received document')
            raw=source.read_bytes();digest=hashlib.sha256(raw).hexdigest();name=Path(item.get('name')or source.name).name;origin={'channel':request.get('channel','manual'),'message_id':request.get('message_id',''),'received_at':request.get('received_at')or (datetime.datetime.fromtimestamp(request['source_timestamp_seconds'],ZoneInfo('Asia/Shanghai')).isoformat()if request.get('source_timestamp_seconds')else datetime.datetime.now(ZoneInfo('Asia/Shanghai')).isoformat())}
            origin['file_name']=name
            known=state.get(digest)
            if known:
                if not any(x.get('channel')==origin['channel']and x.get('message_id')==origin['message_id']for x in known['sources']):known['sources'].append(origin)
                save(base/known['source_record'],json.dumps(known,ensure_ascii=False,indent=2).encode());results.append({**known,'duplicate':True});continue
            name=Path(item.get('name')or source.name).name
            stem=re.sub(r'[\x00-\x1f\\/:*?"<>|\[\]#^]','_',Path(name).stem).strip(' .')[:100]or'未命名资料'
            ext=source.suffix.lower();folder=Path('建筑工程/合同')if any(x in name.lower()for x in ['合同','工程','施工','contract'])else Path('待分类')
            identity=stem+'-'+digest[:12];dest=base/'原件'/digest[:2]/(digest+ext)
            save(dest,raw);text,status=extract(dest)
            if folder==Path('待分类') and any(x in text[:3000]for x in ['施工合同','工程合同','建筑合同','工程名称']):folder=Path('建筑工程/合同')
            note=base/folder/(identity+'.md');textfile=base/'正文'/(digest+'.txt');metadata=base/'来源'/(digest+'.json')
            if text:save(textfile,text.encode('utf-8'))
            meta={'sha256':digest,'original_name':name,'original_path':str(dest.relative_to(base)),'note_path':str(note.relative_to(base)),'text_path':str(textfile.relative_to(base))if text else None,'source_record':str(metadata.relative_to(base)),'sources':[origin],'extraction_status':status,'cloud_policy':'engineering_contracts_allowed_by_user_2026-10-19','review_status':'待核对'}
            fields=[]
            for label in ['工程名称','项目名称','合同编号','甲方','乙方','合同金额','签订日期']:
                m=re.search(r'(?:^|\n)\s*'+label+r'\s*[:：]\s*([^\n]{1,180})',text)
                fields.append('| '+label+' | '+(m.group(1).replace('|','／')if m else'未提取')+' |')
            title=identity;body=text[:160000]if text else'正文尚未成功提取；请按 extraction_status 安排OCR或格式解析，不得声称已经读完。'
            md='---\ntitle: '+json.dumps(title,ensure_ascii=False)+'\ntype: company-document\nstatus: 待核对\ntags: [公司资料库, '+('工程合同'if str(folder)=='建筑工程/合同'else'待分类')+']\nsha256: '+digest+'\nextraction_status: '+status+'\n---\n\n# '+stem+'\n\n[[公司资料库]]\n\n## 原件与来源\n\n- 原件：[['+str(dest.relative_to(vault))+'|'+name.replace(']','_')+']]\n- 来源记录：[['+str(metadata.relative_to(vault))+'|接收记录]]\n- SHA-256：`'+digest+'`\n- 正文提取：`'+status+'`\n- 本批工程合同允许使用配置好的云端模型，不强制本地推理。其他资料沿用其授权范围。\n\n## 合同字段（自动提取，待核对）\n\n| 字段 | 候选值 |\n|---|---|\n'+'\n'.join(fields)+'\n\n## 摘要与关键条款\n\n待助手结合正文整理；金额、期限、责任和版本状态均须核对，缺失不猜测。\n\n## 提取正文\n\n'+body+'\n'
            if len(text)>160000:md+='\n正文较长，笔记仅展示前160000字符；完整正文见 [['+str(textfile.relative_to(vault))+']]。\n'
            save(note,md.encode());save(metadata,json.dumps(meta,ensure_ascii=False,indent=2).encode());state[digest]=meta;results.append({**meta,'duplicate':False})
        save(ledger,json.dumps(state,ensure_ascii=False,indent=2).encode())
        index=base/'公司资料库.md';lines=['---','type: hub','status: active','tags: [公司资料库]','---','','# 公司资料库','','[[AI一人公司知识库]]','','原件、正文、来源和整理笔记分开保存。重复内容按SHA-256去重，不覆盖修订版；扫描件/OCR及未核对字段不算已完成解析。','','## 文件目录','']
        for _,v in sorted(state.items(),key=lambda x:x[1]['original_name']):lines.append('- [['+str((base/v['note_path']).relative_to(vault))+'|'+v['original_name'].replace(']','_')+']] · '+v['extraction_status']+' · 待核对')
        save(index,('\n'.join(lines)+'\n').encode())
        return {'vault':str(vault),'index_path':str(index),'documents':[dict(x,note_path=str(base/x['note_path']),original_path=str(base/x['original_path']),text_path=str(base/x['text_path'])if x['text_path']else None)for x in results]}

def main():
    p=argparse.ArgumentParser();p.add_argument('--request',type=Path,required=True);a=p.parse_args()
    result=ingest(json.loads(a.request.read_text()));print(json.dumps(result,ensure_ascii=False))
if __name__=='__main__':main()
