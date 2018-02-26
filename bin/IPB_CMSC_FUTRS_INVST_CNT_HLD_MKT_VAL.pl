#!/usr/bin/perl
######################################################################
#程序名称:XXXXXXXXX0200.pl
#版本信息:v1.0
#描述简介:
#修改时间             修改人             简述
#
######################################################################      
use strict;
#use warnings FATAL => 'all';
use Encode;
use File::Path;
use Cwd;
use File::Spec::Functions;

unshift(@INC, "$ENV{AUTO_HOME}/bin");
require etl_pub_all;
require etl_pub_btq;
require etl_pub_his;

######################################################################
#Variable Section

######################################################################
#main function

my $DIR = $ARGV[0];
my $str = "";
my $dbname = "";
my $state = "";
my $log_file = ETL_PUB_ALL::openlogfile($DIR);
my $logon_str = ETL_PUB_ALL::getEtlLogonStr("IPB");
print($logon_str);




open SAVEOUT, ">&STDOUT";
open SAVEERR, ">&STDERR";

#记录日志
open(STDOUT, ">>$log_file");
open(STDERR, ">>$log_file");

my $start_time = ETL_PUB_ALL::getTime("yyyymmdd hh:mi:ss");
print "Begin to write log file:$start_time\n";

#数据加载
my $ret = ETL_PUB_BTQ::runBteqCommand();
print "ETL_PUB_BTQ::runBteqCommand() = $ret\n";

close(STDOUT);
close(STDERR);

#日志展示
open STDOUT, ">&SAVEOUT";
open STDERR, ">&SAVEERR";

#执行状态入库
my $r = ETL_PUB_ALL::parseDirInfo($DIR);
my $sys = $r->{SYS};
my $tmp_tab = $r->{JOBNAME};
my $txdate = $r->{TXDATE};
 

my $indc = substr($tmp_tab, 0, 3);
my $jobname = substr($tmp_tab, 4);

if ($indc eq "IPB" or $indc eq "SUM" or $indc eq "KPI" or $indc eq "SPE" or $indc eq "PUB") {
    $dbname = $ETL_PUB_ALL::AUTO_nsPUBMart;
}    #nsPUBMART
elsif ($indc eq "SMS") {$dbname = $ETL_PUB_ALL::AUTO_nsSMSMart;}    #nsSMSMART
elsif ($indc eq "RMS") {$dbname = $ETL_PUB_ALL::AUTO_nsRMSMart;}    #nsRMSMART
elsif ($indc eq "SDM") {$dbname = $ETL_PUB_ALL::AUTO_nsSMSDM;}    #nsSMSDM
elsif ($indc eq "RDS") {$dbname = $ETL_PUB_ALL::AUTO_nsRMSDM;}    #nsRMSDM
elsif ($indc eq "PRD") {$dbname = $ETL_PUB_ALL::AUTO_nsPDATA;}    #nsPDATA


# 导出数据
if ($ret == 0) {
    print "Exporting data ...\n";
    $ret = export_data($jobname,$txdate);
}

my $end_time = ETL_PUB_ALL::getTime("yyyymmdd hh:mi:ss");
print "End to write log file:$end_time\n";

if ($ret == 0) {$state = "success";}
else {$state = "failed";}
my $ret1 = ETL_PUB_ALL::insertLOGtable($sys, $jobname, $dbname, $txdate, $start_time, $end_time, $state);
print "ETL_PUB_ALL::innerLOGtable= $ret1\n";

print "\n";
$str = `sed -n '/Begin to write log file:$start_time/,/End to write log file:$end_time/'p $log_file`;
print "$str";

print "##############################\nlogfile print over!!\n##############################\n";

close(STDOUT);
close(STDERR);

exit($ret);

sub export_data {
    my ($table_name,$txdate) = @_;

    my $work_dir = $ENV{'CMSC_EXPORT_HOME'};
    my $script_dir = catdir($work_dir, "scripts");
    my $data_dir = catdir($work_dir, "data");
    my $log_dir = catdir($work_dir, "logs");
    my ($last_month_begin,$last_month_end,$last_month) = getLastMonthDay($txdate);
    my $datafile_name =  "CMSMC_J0009_V01_". $last_month_end ."_01_Z";
	my $exp_sql = "select \
				     cast(tjrq as varchar(7)), \
					 cast(tzzfl as varchar(5)),\
					 cast(cjsl as varchar(30)),\
					 cast(cjslzb as varchar(8))\
				   from nspubview.CMSC_FUTRS_INVST_CNT_HLD_MKT_VAL\
				   where tjrq = "."'".$last_month."'";
	my $log_table = "nsptemp.fastexp_J0009_log";
    $ENV{"EXPORT_FILE"} = catfile($data_dir,$datafile_name.".tmp");

    my $old_dir = getcwd();

    mkpath($data_dir);
    mkpath($log_dir);

    chdir($data_dir);

    #my $script_file = catfile($script_dir, $table_name.".fexp");
    #my $log_file = catfile($log_dir, $datafile_name.".log");
	print ($exp_sql."\n");

   # my $ret = system("fexp < $script_file 1>$log_file 2>&1");
   my $ret = run_export($log_table,$log_file,$exp_sql);
    if ($ret != 0) {
        chdir($old_dir);
        return $ret;
    }

    my $data_file1 = catfile($data_dir, $datafile_name.".tmp");
    my $data_file2 = catfile($data_dir, $datafile_name.".txt");

    file_convert($data_file1, $data_file2);

    unlink($data_file1);

    chdir($old_dir);
	return $ret;

}

sub file_convert {
    my ($input_file, $output_file) = @_;
    
	if(open(my $fh, $input_file) &&  open(my $fh_out, ">$output_file"))
	{
		while (<$fh>) {
        my $line = encode("utf8", decode("gbk", $_));
        $line =~ s/[ ]+$//;
        $line =~ s/[ ]+/|/g;
        print $fh_out $line;
    }

    close($fh);
    close($fh_out);
	}
	else
	{
		print "can not open input_file";
		return 1;
	}
	return 0;

}


#===================================================
#根据输入的日期信息,得到此日期所在月上个月的第一天和最后一天的日期值,考虑闰年
#日期格式统一是YYYYMMDD,输入信息至少有6位表示年/月信息
#===================================================
sub getLastMonthDay
{
   	my $tmpDate="$_[0]";
   	my @tmpDays=(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);
   	my ($tmpYear,$tmpMonth) = (int(substr($tmpDate,0,4)),(int(substr($tmpDate,4,2))-1));
	($tmpYear,$tmpMonth) = (int(substr($tmpDate,0,4))-1,12) if ((int(substr($tmpDate,4,2))-1) == 0);
   	my ($tmpBgnDate,$tmpEndDate)=(1,$tmpDays[$tmpMonth-1]);
   	$tmpEndDate = 29 if ( $tmpMonth == 2 && ETL_PUB_ALL::isYeapYear($tmpYear));
   	my @tmpMonthDay;
   	$tmpMonthDay[0]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,$tmpBgnDate);
   	$tmpMonthDay[1]=sprintf("%04d%02d%02d",$tmpYear,$tmpMonth,$tmpEndDate);
   	$tmpMonthDay[2]=sprintf("%04d%02d",$tmpYear,$tmpMonth);
   	return @tmpMonthDay;
}



sub run_export
{
    my ($log_table,$log_file,$exp_sql) = @_;

    open(STDOUT, ">>$log_file");
    open(STDERR, ">>$log_file");
    
    my $rc = open(FEXP, "| fexp");
      	unless ($rc) {
        print "Could not invoke fexp command\n";
        return 1;
    }
    
    print FEXP <<ENDOFINPUT;

    .LOGTABLE $log_table;
    $logon_str

    .BEGIN EXPORT
    SESSIONS 4;

   .EXPORT
    OUTMOD /ETL/APP/IPB/LIB/outmod.so
    MODE RECORD
    FORMAT TEXT;

    $exp_sql;
    .END EXPORT;
    .LOGOFF;

ENDOFINPUT

    close(FEXP);
    
    close(STDOUT);
    close(STDERR);
    
    open STDOUT, ">&SAVEOUT";
    open STDERR, ">&SAVEERR";
    
    return 0;
}
