import tempfile,unittest,json,zipfile
from pathlib import Path
from ingest import ingest
class Archive(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.v=Path(self.tmp.name);(self.v/'.obsidian').mkdir();self.file=self.v/'施工合同.txt';self.file.write_text('工程名称：测试工程\n甲方：示例甲\n乙方：示例乙\n合同金额：100元\n',encoding='utf-8')
 def tearDown(self):self.tmp.cleanup()
 def req(self,msg='one'):return {'vault':str(self.v),'channel':'weixin','message_id':msg,'files':[{'path':str(self.file),'name':self.file.name}]}
 def test_original_text_note_and_source_exist(self):
  v=ingest(self.req());d=v['documents'][0];self.assertEqual(Path(d['original_path']).read_bytes(),self.file.read_bytes());self.assertTrue(Path(d['text_path']).is_file());self.assertIn('100元',Path(d['note_path']).read_text());self.assertEqual(d['review_status'],'待核对')
 def test_content_dedup_records_multiple_sources(self):
  a=ingest(self.req())['documents'][0];b=ingest(self.req('two'))['documents'][0];self.assertEqual(a['note_path'],b['note_path']);self.assertTrue(b['duplicate']);self.assertEqual(len(b['sources']),2)
 def test_new_version_keeps_previous_original(self):
  a=ingest(self.req())['documents'][0];self.file.write_text('修订工程合同');b=ingest(self.req('two'))['documents'][0];self.assertNotEqual(a['note_path'],b['note_path']);self.assertTrue(Path(a['original_path']).is_file())
 def test_unparsed_binary_is_preserved_and_marked(self):
  self.file=self.v/'旧合同.bin';self.file.write_bytes(b'\x00\x01');d=ingest(self.req())['documents'][0];self.assertEqual(d['extraction_status'],'format_review');self.assertIsNone(d['text_path'])
 def test_docx_extracted_without_running_embedded_code(self):
  self.file=self.v/'工程合同.docx'
  with zipfile.ZipFile(self.file,'w')as z:z.writestr('word/document.xml','<w:document xmlns:w="word"><w:body><w:p><w:r><w:t>甲方：示例单位</w:t></w:r></w:p></w:body></w:document>')
  d=ingest(self.req())['documents'][0];self.assertIn('示例单位',Path(d['text_path']).read_text())
 def test_invalid_xls_is_preserved_for_review(self):
  self.file=self.v/'broken.xls';self.file.write_bytes(b'not an Excel file')
  d=ingest(self.req())['documents'][0];self.assertIn(d['extraction_status'],['xls_review','xls_extractor_required']);self.assertIsNone(d['text_path']);self.assertEqual(Path(d['original_path']).read_bytes(),b'not an Excel file')
 def test_reprocessing_preserves_existing_note_and_is_idempotent(self):
  from reprocess import reprocess
  from unittest.mock import patch
  self.file=self.v/'old.bin';self.file.write_bytes(b'original')
  d=ingest(self.req())['documents'][0];note=Path(d['note_path']);note.write_text(note.read_text()+'\nHuman annotation\n')
  with patch('reprocess.extract',return_value=('Recovered text','extracted')):
   reprocess(self.v);first=note.read_text();reprocess(self.v)
  self.assertEqual(first,note.read_text());self.assertIn('Human annotation',first);self.assertIn('Recovered text',first)
 def test_pdf_text_uses_available_parser(self):
  try:
   from reportlab.pdfgen import canvas
   import pypdf
  except ImportError:self.skipTest('PDF runtime libraries not available')
  self.file=self.v/'工程合同.pdf';c=canvas.Canvas(str(self.file));c.drawString(50,750,'Contract reference: SYNTHETIC-123');c.save();d=ingest(self.req())['documents'][0];self.assertEqual(d['extraction_status'],'extracted');self.assertIn('SYNTHETIC-123',Path(d['text_path']).read_text())
if __name__=='__main__':unittest.main()
