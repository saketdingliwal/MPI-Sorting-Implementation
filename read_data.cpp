#include <bits/stdc++.h>
#include <mpi.h>

using namespace std;

int get_work(int n,int m)
{
  if(n%m==0)
    return n/m;
  return ((n/m) + 1);
}


int main(int argc,char * argv[])
{
  int rank;
  int num_proc;
  int col_count = atoi(argv[1]);
  int row_count = atoi(argv[2]);
  MPI_Init(&argc,&argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD,&num_proc);
  int col_work = get_work(col_count,num_proc);
  int row_work = get_work(row_count,num_proc);
  int new_row_count = row_work * num_proc;
  int new_col_count = col_work * num_proc;
  float keys_mat[col_work][new_row_count];
  float new_keys_mat[row_work][new_col_count];
  for(int i=0;i<col_work;i++)
  {
    for(int j=0;j<new_row_count;j++)
      keys_mat[i][j] = FLT_MAX; // todo for if input has its own float max
  }
  int my_col = 0;
  for(int ind=rank*col_work;ind<min(col_count,rank*col_work+col_work);ind+=1)
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
    out_string = "./data/cold/col" + ss.str();
    pFile = fopen ( out_string.c_str()  , "rb" );
    if (pFile==NULL) {fputs ("File error",stderr); exit (1);}
    fseek (pFile , 0 , SEEK_END);
    lSize = ftell (pFile);
    rewind (pFile);
    result = fread (n_buffer,4,1,pFile);
    int n = n_buffer[0];
    val_buffer = (char*) malloc (n);
    int num_keys = (lSize-4)/(4+n);
    for(int i=0;i<num_keys;i++)
    {
      result = fread (key_buffer,4,1,pFile);
      keys_mat[my_col][i] = key_buffer[0];
      result = fread (val_buffer,1,n,pFile);
      // cout << val_buffer << endl;
    }
    my_col++;
    fclose (pFile);
  }

  while(true)
  {
    float linearized_col2row[col_work*row_work*num_proc];
    float recd_buff[col_work*row_work*num_proc];
    for(int i=0;i<col_work;i++)
    {
      int start = i * row_work;
      for(int j=0;j<new_row_count;j++)
      {
        linearized_col2row[start] = keys_mat[i][j];
        start++;
        if(start%row_work==0)
        {
          start -= row_work;
          start += (col_work*row_work);
        }
      }
    }
    int success = MPI_Alltoall(linearized_col2row,col_work*row_work,MPI_FLOAT,recd_buff,col_work*row_work,MPI_FLOAT,MPI_COMM_WORLD);
    for(int i=0;i<row_work;i++)
    {
      int start = i;
      for(int j=0;j<new_col_count;j++)
      {
        new_keys_mat[i][j] = recd_buff[start];
        start+=row_work;
      }
    }
    int flag = 0;
    for(int i=0;i<row_work;i++)
    {
      for(int j=1;j<new_col_count;j++)
      {
        if(new_keys_mat[i][j] < new_keys_mat[i][j-1])
        {
          flag =1;
          break;
        }
      }
      sort(new_keys_mat[i],new_keys_mat[i]+new_col_count);
    }
    if(flag==0)
      break;
    for(int i=0;i<row_work;i++)
    {
      int start = i*col_work;
      for(int j=0;j<new_col_count;j++)
      {
        linearized_col2row[start] = new_keys_mat[i][j];
        start++;
        if(start%col_work==0)
        {
          start -= col_work;
          start += (col_work*row_work);
        }
      }
    }

    success = MPI_Alltoall(linearized_col2row,col_work*row_work,MPI_FLOAT,recd_buff,col_work*row_work,MPI_FLOAT,MPI_COMM_WORLD);
    for(int i=0;i<col_work;i++)
    {
      int start = i;
      for(int j=0;j<new_row_count;j++)
      {
        keys_mat[i][j] = recd_buff[start];
        start += col_work;
      }
    }
    flag = 0;
    for(int i=0;i<col_work;i++)
    {
      for(int j=1;j<new_row_count;j++)
      {
        if(keys_mat[i][j] < keys_mat[i][j-1])
        {
          flag =1;
          break;
        }
      }
      sort(keys_mat[i],keys_mat[i]+new_row_count);
    }
    if(flag==0)
      break;
  }
  


  MPI_Finalize();
  return 0;
}
