using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Windows.Forms;
using System.Data;

namespace UnicaSQL
{
    public class DBMS_SQLServer
    {
        SqlConnection conexion = new SqlConnection();
        SqlTransaction tran;
        SqlCommand cmd;
        SqlDataAdapter Adapter;
        //conexionTxt txt;
        public DBMS_SQLServer(string sCadConn)
        {
            conexion.ConnectionString = sCadConn;
        }
        public Boolean Conectarse()
        {
            Boolean bAllOk = false;
            try
            {                
                conexion.Open();
                bAllOk = true;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bAllOk;
        }
        public Boolean Desconectarse()
        {
            Boolean bAllOk = false;
            try
            {
                conexion.Close();
                bAllOk = true;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bAllOk;
        }
        public Boolean Ejecutar(string comando)
        {
            Boolean bAllOk = false;
            try
            {
                if (conexion.State == ConnectionState.Open)
                {
                    cmd = new SqlCommand(comando, conexion);
                    if (transOK)
                    {
                        cmd.Transaction = this.tran;
                    }
                    int n = cmd.ExecuteNonQuery();
                    if (n >0)
                    {
                        bAllOk = true;
                    }
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            return bAllOk;
        }
        public Boolean Ejecutar(String sSQLStmt, ref DataTable Tabla)
        {
            Boolean bAllok = false;
            try
            {
                if (conexion.State == ConnectionState.Open)
                {
                    cmd = new SqlCommand(sSQLStmt, conexion);
                    if (transOK)
                    {
                        cmd.Transaction = this.tran;
                    }
                    Adapter = new SqlDataAdapter();

                    Adapter.SelectCommand = cmd;

                    DataSet ds = new DataSet();
                    
                    int n=Adapter.Fill(ds);
                    if (n > 0)
                    {
                        Tabla = ds.Tables[0];                        
                    }
                    bAllok = true;
                    
                }                
            }
            catch (Exception ex)
            {                
                MessageBox.Show(ex.Message);
            }

            return bAllok;
        }
        Boolean transOK = false;
        public void IniciarTransaccion()
        {            
            tran = conexion.BeginTransaction();   
            transOK = true;
        }
        public void FinTransaccion(Boolean bAllOk)
        {
            if (bAllOk)
            {
                tran.Commit();
            }
            else
            {
                tran.Rollback();
            }
            transOK = false;
        }
    }
}
