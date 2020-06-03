import sys
import threading
import os
import multiprocessing
import time
import thread

mutex = threading.Lock()

class MapReduce():

	def __init__(self):
		self.num_arguments = len(sys.argv)
		self.arguments = sys.argv
		self.incorrect_chars = '.,?:;'
		self.files = []

	def printInformation(self,file_name):
		print '--------------------------'
		print 'File name: ',file_name
		print '--------------------------'
		print '--------------------------'
		print 'NUMBER OF THREADS: ',self.getThreads(file_name)
		print 'SIZE OF FILE: ',os.path.getsize(file_name), ' (bytes) ',(int(os.path.getsize(file_name))/1024),' (kb)' ,(int(os.path.getsize(file_name))/1024)/1024, '(MB)'
		print 'SIZE OF FILE TO READ FOR EACH THREAD (bytes): ',int(os.path.getsize(file_name))/self.getThreads(file_name)
		print '--------------------------'

	def checkTextFiles(self):
		''' funcio que permet comprovar el nombre d'arxius que se'ns passen i compleixen el format: file.txt 
		Si s'hi passa mes d'un fitxer com a parametre, i algun d'ells no compleix el format establert, 
		aquest no sera tractat per la funcio map reduce. Tambe te en compte que no sens pasi un arxiu buit, es a dir, en cas que 
		escrivim com a parametre .txt sense cap nom davant'''

		type_file = ".txt"

		for argument in self.arguments[1:]:
			if str(argument).find(type_file) != -1:
				if argument[0] != ".": #check the case (empty).txt
					self.files.append(argument)

	def checkSize(self,file,size,seek): 
		'''Funcio que calcula si el tamany a llegir del fitxer per cada thread, talla alguna paraula.
	 	En cas que si, s'aplica un bucle que decrementa el numero de bytes totals a llegir fins que no s'incideixi en la sintaxi d'una paraula.
	 	Retorna el nou tamany en bytes.'''
		
		new_size = size

		with open(file,"rb") as f:
		# with: there is no need to do: file.close()

			f.seek(seek) 
			text_to_map = f.read(size)           
			last_char_index = len(text_to_map) - 1
			while (text_to_map[last_char_index] != " "):
				last_char_index -= 1
				new_size -= 1

		# bytes to read from text for each thread.
		return new_size

	def getThreads(self,file):
		
		size = os.path.getsize(file)
		num_t = int(size/100000)
	
		if num_t is 0: num_t = 1

		return num_t
	
	def allTogether(self):
		'''Agrupa tots els fitxers en un. La llista self.files passa a tenir nomes aquest fitxer sobre el qual aplicara
		el map reduce'''

		with open("alltogether.txt","w") as all_file:
			for file in self.files:
				with open (file,"r") as n_file:
					text = n_file.read()
					all_file.write(' '+text)
		self.files = []
		self.files.append("alltogether.txt")

	def setThreads(self,file):
		''' Funcio que calcula els parametres per la funcio map i crea N threads que executraran el map().
		En aquesta funcio els parametres depenen del tamany en bytes del tros que volem que un thread analitzi. Es el parametre 
		self.size '''

		num_threads = self.getThreads(file)
		total_size = os.path.getsize(file)
		size_thread = total_size/num_threads

		if 'i' in self.arguments:
			self.printInformation(file)
		
		pos_start_read = 0
		size_to_read = 0

		for iteration in range(num_threads):
			
			if iteration + 1 == num_threads:
				#last thread reads left data
				size_to_read = total_size - pos_start_read
			else:
				size_to_read = self.checkSize(file,size_thread,pos_start_read)

			thread = threading.Thread(target = self.map, args = (file,size_to_read,pos_start_read))
			thread.run()
	
			size_to_read += pos_start_read
			pos_start_read = size_to_read

		self.reduce(file)
		os._exit(0) #acaba el proces

		''' MAP REDUCE'''

	def splitting(self,filename,size,seek):
		
		with open(filename,"r+") as file:
			
			file.seek(seek)
			text = []

			for word in file.read(size).split():
				text.append(word.lower().translate(None,self.incorrect_chars))
	
		return text

	def map(self,filename,size,seek):
		''' Funcio map'''
		
		text = self.splitting(filename,size,seek)
		
		for word in text:
			text_to_shuffle.append([word,1])

		self.shuffling()
	
	def shuffling(self):

		global text_to_reduce
		global text_to_shuffle
		
		for parella in text_to_shuffle:
			if parella[0] not in text_to_reduce:
				text_to_reduce[parella[0]] = [parella[1]]
			else:
				values_list = text_to_reduce[parella[0]]
				values_list.append(parella[1])
				text_to_reduce[parella[0]] = values_list

		text_to_shuffle = []
		
	def reduce(self,file):

		global text_to_reduce

		for key in text_to_reduce:
			total = 0
			for value in text_to_reduce[key]:
				total += value
			text_to_reduce[key] = total

		print str(file)+':'
		for key in text_to_reduce:
			print key,':',text_to_reduce[key]
		print '\n'

	def mapReduce(self):
		''' fem tants processos com fitxers, i cada proces llensa tants threads com determini l'algoritme self.getThreads '''
		
		global text_to_reduce
		global text_to_shuffle
		
		text_to_reduce = {}
		text_to_shuffle = []

		self.checkTextFiles()

		if 'all' in self.arguments:
			self.allTogether()
			
		for num_files in range(0,len(self.files)):
			pid = os.fork()
			if pid is 0:					
				self.setThreads(self.files[num_files])
			else:
				os.waitpid(pid,0)
							

if __name__ == '__main__':
	
	'''
	opcio = input('(1) Write \n(2) MapReduce\n')
	if opcio is 1:
		with open("testOriginal.txt","a") as file:
			file.write('NOU MAP REDUCE: provant amb arxiu:'+str(sys.argv[1])+' que te tamany: '+str(os.path.getsize(sys.argv[1]))+'\n')
			file.write('TAMANY------NUM-THREADS------TEMPS')
			minim = [100000000000000,0,0]
			contador = 100000
			a = 0
			while contador >= 1:
				num_threads = int((os.path.getsize(sys.argv[1])/(contador)))
				
				llista = []
				for itreration in range(3):
					
					start = time.time()
					map_reduce = MapReduce(size = contador)
					map_reduce.mapReduce()
					end = time.time()
					llista.append(end-start)
					file.write(str(contador)+'   '+str(num_threads)+'   '+str(end-start)+'\n\n')
					#file.write( '---------------------------------\n')
				#file.write(str(contador)+'   '+str(num_threads)+'   '+str(end-start)+'\n\n')

				file.write('Temps-minim: '+str(min(llista))+' Temps-maxim: '+str(max(llista))+' Temps-Mitja: '+str(sum(llista)/3)+'\n\n')
				if min(llista) < minim[0]:
					minim = [min(llista),num_threads,contador]
					a = min(llista)
				llista = []

				contador -= 10000
			file.write('MILLOR: '+str(minim[0])+' num-threads: '+str(minim[1])+' tamany-thread: '+str(minim[2]))

			file.close()

	elif opcio is 2:
		start = time.time()
		'''
	map_reduce = MapReduce()
	map_reduce.mapReduce()

	end = time.time()
		

	#print 'Time: ',end-start
	
	

			

		








