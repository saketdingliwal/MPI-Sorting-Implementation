#include <bits/stdc++.h>
#include <mpi.h>

using namespace std;

float inf = numeric_limits<float>::infinity();

int get_work(int n,int m)
{
  if(n%m==0)
    return n/m;
  return ((n/m) + 1);
}
typedef struct data
{
  float key;
  vector <char> value;
  int n;
}data;

bool datacompare(data lhs, data rhs) {return lhs.key < rhs.key;}

int main(int argc,char * argv[])
{
  int rank;
  int num_proc;
  int col_count = atoi(argv[1]);

  MPI_Init(&argc,&argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD,&num_proc);
  int col_work = get_work(col_count,num_proc);
  int new_col_count = col_work * num_proc;
  int n_s = 0;
  int rows = 0;
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
    n_s = max(n_s,n);
    val_buffer = (char*) malloc (n);
    int num_keys = (lSize-4)/(4+n);
    rows = max(rows,num_keys);
    my_col++;
  }
  int max_n_s[num_proc];
  int max_rows[num_proc];
  MPI_Allgather(&n_s,1,MPI_INT,max_n_s,1,MPI_INT,MPI_COMM_WORLD);
  MPI_Allgather(&rows,1,MPI_INT,max_rows,1,MPI_INT,MPI_COMM_WORLD);
  int max_n = 0;
  int max_row = 0;
  for(int i=0;i<num_proc;i++)
  {
    max_n = max(max_n,max_n_s[i]);
    max_row = max(max_row,max_rows[i]);
  }
  int row_count = max_row;
  int row_work = get_work(row_count,num_proc);
  int new_row_count = row_work * num_proc;
  data keys_mat[col_work][new_row_count];
  // float keys_mat[col_work][new_row_count];
  data new_keys_mat[row_work][new_col_count];
  // float new_keys_mat[row_work][new_col_count];
  for(int i=0;i<col_work;i++)
  {
    for(int j=0;j<new_row_count;j++)
      keys_mat[i][j].key = inf; // todo for if input has its own float max
  }
  my_col = 0;
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
      keys_mat[my_col][i].key = key_buffer[0];
      result = fread (val_buffer,1,n,pFile);
      // cout << val_buffer << endl;
    }
    my_col++;
    fclose (pFile);
  }

  int hagga = 0;
  while(true)
  {
    if(rank==0)
      cout << hagga << endl;
    hagga++;
    float linearized_col2row[col_work*row_work*num_proc];
    float recd_buff[col_work*row_work*num_proc];
    for(int i=0;i<col_work;i++)
    {
      int start = i * row_work;
      for(int j=0;j<new_row_count;j++)
      {
        linearized_col2row[start] = keys_mat[i][j].key;
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
        new_keys_mat[i][j].key = recd_buff[start];
        start+=row_work;
      }
    }
    int flag = 0;
    for(int i=0;i<row_work;i++)
    {
      if(flag==0)
      {
        for(int j=1;j<new_col_count;j++)
        {
          if(new_keys_mat[i][j].key < new_keys_mat[i][j-1].key)
          {
            flag =1;
            break;
          }
        }
      }
      sort(new_keys_mat[i],new_keys_mat[i]+new_col_count,datacompare);
    }
    int flag_array[num_proc];
    MPI_Allgather(&flag,1,MPI_INT,flag_array,1,MPI_INT,MPI_COMM_WORLD);
    int main_flag = 0;
    for(int i=0;i<num_proc;i++)
    {
      main_flag = (main_flag || flag_array[i]);
    }
    if(main_flag==0)
      break;
    // for(int i=0;i<row_work;i++)
    // {
    //   for(int j=0;j<new_col_count;j++)
    //   {
    //     if(rank==0)
    //     cout << new_keys_mat[i][j].key << " ";
    //   }
    //   if(rank==0)
    //   cout << endl;
    // }
    for(int i=0;i<row_work;i++)
    {
      int start = i*col_work;
      for(int j=0;j<new_col_count;j++)
      {
        linearized_col2row[start] = new_keys_mat[i][j].key;
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
        keys_mat[i][j].key = recd_buff[start];
        start += col_work;
      }
    }
    for(int i=0;i<col_work;i++)
    {
      sort(keys_mat[i],keys_mat[i]+new_row_count,datacompare);
    }
  }

  MPI_Finalize();
  return 0;
}
