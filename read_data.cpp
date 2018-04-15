#include <bits/stdc++.h>
#include <mpi.h>
#include <sys/stat.h>

using namespace std;

float inf = numeric_limits<float>::infinity();

long long int get_work(long long int n,long long int m)
{
  if(n%m==0)
    return n/m;
  return ((n/m) + 1);
}
typedef struct data
{
  float key;
  vector <char> value;
}data;

bool datacompare(data lhs, data rhs) {return lhs.key < rhs.key;}

vector<vector<data> >keys_mat;
vector<vector<data> >new_keys_mat;
vector<data> colm;

int main(int argc,char * argv[])
{
	int rank_;
	int num_proc_;
  long long int col_count = atoi(argv[1]);
  string file_name = argv[2];

  MPI_Init(&argc,&argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
  MPI_Comm_size(MPI_COMM_WORLD,&num_proc_);
  long long int rank = (long long int)rank_;
  long long int num_proc = (long long int)num_proc_;
  long long int col_work = get_work(col_count,num_proc);
  long long int new_col_count = col_work * num_proc;
  int n_s = 0;
  int rows = 0;
  long long int my_col = 0;
  for(long long int ind=rank*col_work;ind<min(col_count,rank*col_work+col_work);ind+=1)
  {
    FILE * pFile;
    long lSize;
    int * n_buffer;
    float * key_buffer;
    char *val_buffer;
    n_buffer = (int*) malloc (4);
    key_buffer = (float*) malloc(4);
    size_t result;
    string out_string;
    stringstream ss;
    ss << (ind+1);
    out_string = file_name + ss.str();
    pFile = fopen ( out_string.c_str()  , "rb" );
    if (pFile==NULL) {fputs ("File error",stderr); exit (1);}
    struct stat st;
		stat(out_string.c_str(), &st);
		lSize = st.st_size;
    // fseek (pFile , 0 , SEEK_END);
    // lSize = ftell (pFile);
    // rewind (pFile);
    result = fread (n_buffer,4,1,pFile);
    int n = n_buffer[0];
    n_s = max(n_s,n);
    val_buffer = (char*) malloc (n);
    int num_keys = (lSize-4)/(4+n);
    rows = max(rows,num_keys);
    fclose (pFile);
    my_col++;
  }
  int max_n_s[num_proc];
  int max_rows[num_proc];
  MPI_Allgather(&n_s,1,MPI_INT,max_n_s,1,MPI_INT,MPI_COMM_WORLD);
  MPI_Allgather(&rows,1,MPI_INT,max_rows,1,MPI_INT,MPI_COMM_WORLD);
  long long int max_n = 1;
  long long int max_row = 0;
  for(long long int i=0;i<num_proc;i++)
  {
    max_n = max(max_n,(long long int)max_n_s[i]);
    max_row = max(max_row,(long long int)max_rows[i]);
  }
  long long int row_count = max_row;
  long long int row_work = get_work(row_count,num_proc);
  long long int new_row_count = row_work * num_proc;
  // data keys_mat[col_work][new_row_count];
  // float keys_mat[col_work][new_row_count];
  // data new_keys_mat[row_work][new_col_count];
  // float new_keys_mat[row_work][new_col_count];

  for(long long int i=0;i<col_work;i++)
  {
    colm.clear();
    for(long long int j=0;j<new_row_count;j++)
    {
      data elem;
      elem.key = inf; // todo for if input has its own float max
      for(long long int k=0;k<max_n;k++)
      	elem.value.push_back('\0');
      colm.push_back(elem);
    }
    keys_mat.push_back(colm);
  }

  for(long long int i=0;i<row_work;i++)
  {
    vector<data> colm;
    for(long long int j=0;j<new_col_count;j++)
    {
      data elem;
      elem.key = inf; // todo for if input has its own float max
      for(long long int k=0;k<max_n;k++)
      	elem.value.push_back('\0');
      colm.push_back(elem);
    }
    new_keys_mat.push_back(colm);
  }

  my_col = 0;
  for(long long int ind=rank*col_work;ind<min(col_count,rank*col_work+col_work);ind+=1)
  {
    FILE * pFile;
    long lSize;
    int * n_buffer;
    float * key_buffer;
    char *val_buffer;
    n_buffer = (int*) malloc (4);
    key_buffer = (float*) malloc(4);
    size_t result;
    string out_string;
    stringstream ss;
    ss << (ind+1);
    out_string = file_name + ss.str();
    pFile = fopen ( out_string.c_str()  , "rb" );
    if (pFile==NULL) {fputs ("File error",stderr); exit (1);}
    struct stat st;
		stat(out_string.c_str(), &st);
		lSize = st.st_size;
    result = fread (n_buffer,4,1,pFile);
    int n = n_buffer[0];
    val_buffer = (char*) malloc (n);
    int num_keys = (lSize-4)/(4+n);
    for(long long int i=0;i<num_keys;i++)
    {
      result = fread (key_buffer,4,1,pFile);
      keys_mat[my_col][i].key = key_buffer[0];
      result = fread (val_buffer,1,n,pFile);
      for(long long int k=0;k<max_n;k++)
      {
        // if(k<n)
          keys_mat[my_col][i].value[k] = (val_buffer[k]);
        // else
          // keys_mat[my_col][i].value.push_back('\0');
      }
    }
    my_col++;
    fclose (pFile);
  }
  long long int savee = ((long long int) 1) * col_work*row_work*num_proc;
  long long int save2 = ((long long int) 1) * col_work*row_work*num_proc*max_n;

  float *linearized_col2row;
  float *recd_buff;
  char *linearized_col2row_value;
  char *recd_buff_value;

  linearized_col2row = (float *) malloc(sizeof(float)*savee);
  recd_buff = (float *) malloc(sizeof(float)*savee);
  linearized_col2row_value = (char *)malloc(sizeof(char)*save2);
  recd_buff_value = (char *)malloc(sizeof(char)*save2);

  // float linearized_col2row[col_work*row_work*num_proc];
  // char linearized_col2row_value[max_n*col_work*row_work*num_proc];
  // float recd_buff[col_work*row_work*num_proc];
  // char recd_buff_value[max_n*col_work*row_work*num_proc];

  long long int main_bug = 0;
  while(true)
  {
    for(long long int i=0;i<col_work;i++)
    {
      long long int start = ((long long int) 1) * i * row_work;
      for(long long int j=0;j<new_row_count;j++)
      {
        linearized_col2row[start] = keys_mat[i][j].key;
        for(long long int k=0;k<max_n;k++)
        {
          // if(keys_mat[i][j].value.size()<max_n)
            // keys_mat[i][j].value.push_back('\0');
          linearized_col2row_value[start*max_n + k] = keys_mat[i][j].value[k];
        }
        start++;
        if(start%row_work==0)
        {
          start -= row_work;
          start += (col_work*row_work);
        }
      }
    }
    long long int success = MPI_Alltoall(linearized_col2row,col_work*row_work,MPI_FLOAT,recd_buff,col_work*row_work,MPI_FLOAT,MPI_COMM_WORLD);
    success = MPI_Alltoall(linearized_col2row_value,max_n*col_work*row_work,MPI_CHAR,recd_buff_value,max_n*col_work*row_work,MPI_CHAR,MPI_COMM_WORLD);
    for(long long int i=0;i<row_work;i++)
    {
      long long int start = ((long long int) 1) * i;
      for(long long int j=0;j<new_col_count;j++)
      {
        new_keys_mat[i][j].key = recd_buff[start];
        for(long long int k=0;k<max_n;k++)
        {
          char recd = recd_buff_value[start*max_n + k];
          // if(new_keys_mat[i][j].value.size()==0)
            // new_keys_mat[i][j].value.resize(max_n);
          new_keys_mat[i][j].value[k] = recd;
        }
        start+=row_work;
      }
    }
    int flag = 0;
    for(long long int i=0;i<row_work;i++)
    {
      if(flag==0)
      {
        for(long long int j=1;j<new_col_count;j++)
        {
          if(new_keys_mat[i][j].key < new_keys_mat[i][j-1].key)
          {
            flag =1;
            break;
          }
        }
      }
      sort(new_keys_mat[i].begin(),new_keys_mat[i].end(),datacompare);
    }
    int flag_array[num_proc];
    MPI_Allgather(&flag,1,MPI_INT,flag_array,1,MPI_INT,MPI_COMM_WORLD);
    long long int main_flag = 0;
    for(long long int i=0;i<num_proc;i++)
    {
      main_flag = (main_flag || flag_array[i]);
    }
    if(main_flag==0 && main_bug==1)
      break;
    main_bug = 1;
    for(long long int i=0;i<row_work;i++)
    {
      long long int start = ((long long int) 1) * i*col_work;
      for(long long int j=0;j<new_col_count;j++)
      {
        linearized_col2row[start] = new_keys_mat[i][j].key;
        for(long long int k=0;k<max_n;k++)
        {
          linearized_col2row_value[start*max_n + k] = new_keys_mat[i][j].value[k];
        }
        start++;
        if(start%col_work==0)
        {
          start -= col_work;
          start += (col_work*row_work);
        }
      }
    }
    success = MPI_Alltoall(linearized_col2row,col_work*row_work,MPI_FLOAT,recd_buff,col_work*row_work,MPI_FLOAT,MPI_COMM_WORLD);
    success = MPI_Alltoall(linearized_col2row_value,max_n*col_work*row_work,MPI_CHAR,recd_buff_value,max_n*col_work*row_work,MPI_CHAR,MPI_COMM_WORLD);
    for(long long int i=0;i<col_work;i++)
    {
      long long int start = ((long long int) 1) * i;
      for(long long int j=0;j<new_row_count;j++)
      {
        keys_mat[i][j].key = recd_buff[start];
        for(long long int k=0;k<max_n;k++)
        {
          char recd = recd_buff_value[start*max_n + k];
          keys_mat[i][j].value[k] = recd;
        }
        start += col_work;
      }
    }
    for(long long int i=0;i<col_work;i++)
    {
      sort(keys_mat[i].begin(),keys_mat[i].end(),datacompare);
    }
  }
  MPI_Status status;
  if(rank==0)
  {
  	FILE * pFile;
  	string filee = file_name + "0";
  	pFile = fopen (filee.c_str(), "wb");
  	for(long long int procno=1;procno<=num_proc;procno++)
  	{
  		// cout << "proc " << procno-1 <<endl;
  		for(long long int i=0;i<row_work;i++)
		  {
		    for(long long int j=0;j<new_col_count;j++)
		    {
		      if(new_keys_mat[i][j].key!=inf)
		      {
	  				fwrite(&new_keys_mat[i][j].key,4,1,pFile);
	  				cout << new_keys_mat[i][j].key << " ";
			      for(long long int k=0;k<max_n;k++)
			      {
			        if(new_keys_mat[i][j].value[k]!='\0')
			        {
			        	cout << new_keys_mat[i][j].value[k];
			          fwrite(&new_keys_mat[i][j].value[k],1,1,pFile);
			        }
			        else
			        	break;
			      }
			      cout << endl;
			    }
		    }
		  }
			if(procno==num_proc)
				break;
			MPI_Recv(recd_buff,savee,MPI_FLOAT,procno,0,MPI_COMM_WORLD,&status);
			MPI_Recv(recd_buff_value,save2,MPI_CHAR,procno,1,MPI_COMM_WORLD,&status);
			for(long long int i=0;i<row_work;i++)
	    {
	      long long int start = ((long long int) 1) * i;
	      for(long long int j=0;j<new_col_count;j++)
	      {
	        new_keys_mat[i][j].key = recd_buff[start];
	        for(long long int k=0;k<max_n;k++)
	        {
	          char recd = recd_buff_value[start*max_n + k];
	          // if(new_keys_mat[i][j].value.size()==0)
	            // new_keys_mat[i][j].value.resize(max_n);
	          new_keys_mat[i][j].value[k] = recd;
	        }
	        start+=row_work;
	      }
	    }
  	}
  	fclose (pFile);
  }
  else
  {
		MPI_Send(recd_buff,savee,MPI_FLOAT,0,0,MPI_COMM_WORLD);
		MPI_Send(recd_buff_value,save2,MPI_CHAR,0,1,MPI_COMM_WORLD);
  }

  // long long int byte_counter = 0;
  // for(long long int i=0;i<row_work;i++)
  // {
  //   for(long long int j=0;j<new_col_count;j++)
  //   {
  //     if(new_keys_mat[i][j].key!=inf)
  //       byte_counter +=4;
  //     for(long long int k=0;k<max_n;k++)
  //     {
  //       if(new_keys_mat[i][j].value[k]!='\0')
  //         byte_counter +=1;
  //     }
  //   }
  // }
  // long long int bytes_seek[num_proc];
  // long long int pref[num_proc];
  // pref[0] = 0;
  // MPI_Allgather(&byte_counter,1,MPI_INT,bytes_seek,1,MPI_INT,MPI_COMM_WORLD);
  // for(long long int i=1;i<num_proc;i++)
  // {
  //   pref[i] = bytes_seek[i-1] + pref[i-1];
  // }

  // MPI_Status status;
  // MPI_File fh;
  // string filee = file_name + "01";
  // MPI_File_open(MPI_COMM_WORLD,filee.c_str(),MPI_MODE_CREATE | MPI_MODE_WRONLY,MPI_INFO_NULL,&fh);
  // MPI_File_seek(fh, pref[rank], MPI_SEEK_SET);
  // for(long long int i=0;i<row_work;i++)
  // {
  //   for(long long int j=0;j<new_col_count;j++)
  //   {
  //     if(new_keys_mat[i][j].key!=inf)
  //       MPI_File_write(fh,&new_keys_mat[i][j].key,1,MPI_FLOAT,&status);
  //     for(long long int k=0;k<max_n;k++)
  //     {
  //       if(new_keys_mat[i][j].value[k]!='\0')
  //         MPI_File_write(fh,&new_keys_mat[i][j].value[k],1,MPI_CHAR,&status);
  //     }
  //   }
  // }
  MPI_Finalize();
  return 0;
}
