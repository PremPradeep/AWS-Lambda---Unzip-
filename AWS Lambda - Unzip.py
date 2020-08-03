import io
import zipfile
import boto3
import os
from datetime import datetime

def lambda_handler(event, context):    
    
	zip_key = 'gnaf.zip'

	
	filename_fld, file_extension = os.path.splitext(zip_key)

	
	s3_resource = boto3.resource("s3")
	s3_object = s3_resource.Object(bucket_name="itd-prem", key=zip_key)
	
	s3_file = S3File(s3_object)
	
			
	z = zipfile.ZipFile(s3_file)
	
	dir_name = 'Standard/' 
	
	dir_name2 = 'Authority Code/'
	
	
	for filepath in z.namelist():
		print(filepath)
		if dir_name in filepath and filepath.endswith('.psv'):
			filename_w_ext = os.path.basename(filepath)
			filename, file_extension = os.path.splitext(filename_w_ext)
			if not filename_w_ext.startswith('.'):
				print(filename_w_ext)
				s3_resource.meta.client.upload_fileobj(z.open(filepath),Bucket='itd-prem',Key= 'gnaf_files/' + filename + '/' + filename_w_ext)
		if dir_name2 in filepath and filepath.endswith('.psv'):
				filename_w_ext = os.path.basename(filepath)
				filename, file_extension = os.path.splitext(filename_w_ext)
				if not filename_w_ext.startswith('.'):
					print(filename_w_ext)
					s3_resource.meta.client.upload_fileobj(z.open(filepath),Bucket='itd-prem',Key= 'gnaf_files/' + filename + '/' + filename_w_ext)

	glue_client = boto3.client('glue')
	glue_client.start_crawler(Name='gnaf_tables')


class S3File(io.RawIOBase):
	def __init__(self, s3_object):
		self.s3_object = s3_object
		self.position = 0

	def __repr__(self):
		return "<%s s3_object=%r>" % (type(self).__name__, self.s3_object)

	@property
	def size(self):
		return self.s3_object.content_length

	def tell(self):
		return self.position

	def seek(self, offset, whence=io.SEEK_SET):
	    if whence == io.SEEK_SET:
	        self.position = offset
	    elif whence == io.SEEK_CUR:
	        self.position += offset
	    elif whence == io.SEEK_END:
	        self.position = self.size + offset
	    else:
	        raise ValueError("invalid whence (%r, should be %d, %d, %d)" % (
	            whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END
	        ))
	
	    return self.position

	def seekable(self):
	    return True
	
	def read(self, size=-1):
	    if size == -1:
	        # Read to the end of the file
	        range_header = "bytes=%d-" % self.position
	        self.seek(offset=0, whence=io.SEEK_END)
	    else:
	        new_position = self.position + size
	
	        # If we're going to read beyond the end of the object, return
	        # the entire object.
	        if new_position >= self.size:
	            return self.read()
	
	        range_header = "bytes=%d-%d" % (self.position, new_position - 1)
	        self.seek(offset=size, whence=io.SEEK_CUR)
	
	    return self.s3_object.get(Range=range_header)["Body"].read()
	
	def readable(self):
	    return True