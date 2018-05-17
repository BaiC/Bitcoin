using Dapper;
using MySql.Data.MySqlClient;
using Quartz;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataStorage
{
    public class RequestJob : IJob
    {
        public async Task Execute(IJobExecutionContext context)
        {
            //await Console.Out.WriteLineAsync($"Greetings from HelloJob! {context.MergedJobDataMap["conStr"]}");
            try
            {
                //JobKey key = context.JobDetail.Key;

                JobDataMap dataMap = context.JobDetail.JobDataMap;


                using (IDbConnection con = new MySqlConnection(dataMap.GetString("conStr")))
                {
                    var re = await con.ExecuteAsync($"insert into zzz_test set click = 'click'");
                    await Console.Out.WriteLineAsync($"Job {re}");
                }
            }
            catch (Exception ex)
            {
                await Console.Error.WriteLineAsync(ex.ToString());
            }
        }
    }
}
