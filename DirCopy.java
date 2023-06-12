import java.io.*;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.concurrent.*;

/**
 * 实现某一文件夹向多个磁盘分发的程序。
 * 要求：
 * 1.可以自由设置刷新间隔，最低1秒，定时检测指定的文件夹下是否更新了待传输的文件；
 * 2.可以自由设置读取文件夹的目录，和多个写入磁盘的目录；
 * 3.当磁盘当前正存在有写入任务时，或磁盘空间不足时，自动向下一个磁盘传输文件，即每个磁盘同时仅可传输一个文件；当所有磁盘都处于正在写入时，停止传输，待有磁盘空闲时再进行传输；
 * 4.一个文件仅可被传输给一个磁盘；
 * 5.文件名支持以通配符的形式来设置。Windows下的软件
 */

public class DirCopy {

    private final int refreshInterval; // 刷新磁盘列表，默认1s
    private final int maxThreads;  // 最大线程数
    private final String srcDirPath;  // 源目录
    private final String finishedLogPath;
    private final boolean recursive; // 递归扫描
    private final boolean resume;
    private final boolean loop;
    private final ThreadPoolExecutor executor;
    private final HashSet<File> dirSet=new HashSet<>();
    private final ConcurrentLinkedQueue<String> dstDirStack
            = new ConcurrentLinkedQueue<>(); // 空闲磁盘队列

    private final BlockingQueue<String> taskFileList=new LinkedBlockingQueue<>(1000);
//    private final ConcurrentLinkedQueue<String> finishedFileList = new ConcurrentLinkedQueue<>();
    private final ConcurrentSkipListSet<String> allFileSet = new ConcurrentSkipListSet<>();
    private final ScheduledThreadPoolExecutor scheduledExec = new ScheduledThreadPoolExecutor(1); // 单线程更新

    public DirCopy() {

        File[] roots = File.listRoots();
        printDisksInfo(roots);
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入读取文件夹的完整路径：(例如: /opt/)");
        this.srcDirPath = scanner.nextLine();

        // 设置 dstDir
        System.out.println("请输入写入的文件夹路径,多个路径用空格隔开 :");
        String[] tmpDirList= scanner.nextLine().split(" ");
        for(String tmpDir: tmpDirList) {
            if(tmpDir != "" && !tmpDir.isEmpty()) {
                dstDirStack.add(tmpDir);
                new File(tmpDir).mkdirs();
            }
        }

        System.out.println("\n请输入最大线程数（例如：20）:");
        this.maxThreads=scanner.nextInt();
        scanner.nextLine();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);

        Path path = Paths.get(srcDirPath,"finishedFiles.txt");
        finishedLogPath=path.toString();
        try {
            File finishedLog = new File(finishedLogPath);
            Scanner logScanner = new Scanner(finishedLog);
            while (logScanner.hasNextLine()) {
                allFileSet.add(logScanner.nextLine());
            }
        } catch (FileNotFoundException e) {
            ; // can't do anything
        }
        System.out.println("是否递归扫描所有子目录?\nY/N: ");
        this.recursive = sayYes(scanner.nextLine());

        System.out.println("是否保留已复制完成的文件记录，下次跳过？ \nY/N: ");
        this.resume = sayYes(scanner.nextLine());

        System.out.println("是否定时循环扫描源目录？\nY/N:");
        this.loop=sayYes(scanner.nextLine());

        System.out.println("\n请输入刷新间隔：(例如：1 代表1s）");
        this.refreshInterval=scanner.nextInt();
    }

    private boolean sayYes(String str) {
        return (str.equals("Y") || str.equals("y")) ;
    }

    public DirCopy(int refreshInterval, int maxThreads, String srcDirPath,
             boolean recursive, boolean resume, boolean loop,LinkedList<String> dstDirList ) {
        File[] roots = File.listRoots();
        printDisksInfo(roots);
        Scanner scanner = new Scanner(System.in);
        this.srcDirPath = srcDirPath;

        // 设置 dstDir
        for(String tmpDir: dstDirList) {
            if(tmpDir != "" && !tmpDir.isEmpty()) {
                dstDirStack.add(tmpDir);
                new File(tmpDir).mkdirs();
            }
        }

        this.refreshInterval=refreshInterval;
        this.maxThreads=maxThreads;
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);

        Path path = Paths.get(srcDirPath,"finishedFiles.txt");
        finishedLogPath=path.toString();
        try {
            File finishedLog = new File(finishedLogPath);
            Scanner logScanner = new Scanner(finishedLog);
            while (logScanner.hasNextLine()) {
                allFileSet.add(logScanner.nextLine());
            }
        } catch (FileNotFoundException e) {
            ; // can't do anything
        }
        this.recursive=recursive;
        this.resume=resume;
        this.loop=loop;
    }

    private static double byteToGB(long bytes) {return bytes/1024.0/1024/1024;}

    public static void main(String[] args) throws InterruptedException {
        int refreshInterval=0, maxThreads=0;
        String srcDirPath = null;
        boolean recursive=false, resume=true, tmploop=false, interact=false;
        LinkedList<String> dstDirList=new LinkedList<>();

        if(args.length<12) {
            interact=true;
            System.out.println("缺少必要参数,问答输入!");
            DirCopy.help();
        }
        for(int i=0;i<args.length;i+=2) {
            switch (args[i]) {
                case "--interval":
                    refreshInterval=Integer.parseInt(args[i+1]);
                    break;
                case "--threads":
                    maxThreads = Integer.parseInt(args[i + 1]);
                    break;
                case "--src":
                    srcDirPath = String.valueOf(args[i + 1]);
                    break;
                case "--recursive":
                    recursive = Boolean.parseBoolean(args[i + 1]);
                    break;
                case "--resume":
                    resume = Boolean.parseBoolean(args[i + 1]);
                    break;
                case "--dst":
                    dstDirList.add(args[i + 1]);
                    break;
                case "--loop":
                    tmploop = Boolean.parseBoolean(args[i + 1]);
                    break;
                default:
                    DirCopy.help();
                    System.out.println("No such option: "+args[i]);
                    return;
            }
        }

        DirCopy dirCopy=null;
        if(interact) dirCopy=new DirCopy();
        else dirCopy=new DirCopy(refreshInterval,maxThreads,srcDirPath,recursive,resume,tmploop,dstDirList);
        System.out.println(dirCopy);
        dirCopy.startCopy();

    }

    private static void help() {
        System.out.println("--interval  The refresh interval determains how often to scan the source directory"); // 刷新磁盘列表，默认1s
        System.out.println("--threads  the max count of the copy threads");  // 最大线程数
        System.out.println("--src  the source directory's path");  // 源目录
        System.out.println("--recursive true/false  determain whether recursively scan the source directory"); // 递归扫描
        System.out.println("--resume true/false  determain whether save the finished file name to a log to skip it next time");
        System.out.println("--loop true/false  default:false, determain whether scan source dir in a loop with a fixed delay");
        System.out.println("--dst  which directory the file copied to, can be more than one destiny directory but better not on the same disk");
        System.out.println("eg: java DirCopy --interval 20 --threads 1 --src /G/Music --recursive true --resume true --dstDir /E/DirCopy");
    }


    private void scanSrcDir() throws InterruptedException {
        System.out.println("源目录:"+this.srcDirPath);
        File srcDirFile = new File(srcDirPath);
        File[] allFiles=srcDirFile.listFiles();
//        System.out.println("allFileSet size:"+allFileSet.size());

        for (File file : allFiles) {
            if(file.isFile() ){
                addIfAbsent(file);
            } else {
                dirSet.add(file);
            }
        }

        if(recursive) {
            while (dirSet.isEmpty() == false) {
                File file = dirSet.iterator().next();
                dirSet.remove(file);
                if (file.isFile()) {
                    addIfAbsent(file);
                } else {
                    File[] tmpFiles=file.listFiles();
                    for(File afile:tmpFiles) {
                        if(afile.isFile()) {
                            addIfAbsent(afile);
                        }
                        else dirSet.add(afile);
                    }
                }
            }
        }
//        System.out.println("allFileSet size:"+allFileSet.size());
    }

    private void addIfAbsent(File file) throws InterruptedException {
        String fname=file.toString().trim();
        if ((allFileSet.add(fname)) && (!fname.equals(finishedLogPath)) ) {
            System.out.println("add: "+file);
            taskFileList.offer(fname,1000,TimeUnit.SECONDS);
        }    
    }
    
    public void startCopy() throws InterruptedException {
        // 开启定时刷新任务
        if(loop) {
            scheduledExec.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        scanSrcDir();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, 0, refreshInterval, TimeUnit.SECONDS);
        } else {
            scheduledExec.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        scanSrcDir();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            scheduledExec.shutdown();
        }

        int threadId=0;
        try{
            while (!dstDirStack.isEmpty()) {
                int finalThreadId = threadId;
                System.out.println(threadId++);
                String dstDir=dstDirStack.poll();
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        copyWorker(finalThreadId,dstDir);
                    }
                });
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(refreshInterval*2,TimeUnit.SECONDS);
        }

    }

    private void writeStringToFile(String filename,String str) {
        FileWriter fw = null;
        BufferedWriter bw = null;
        PrintWriter pw = null;
        try {
            fw = new FileWriter(filename, true);
            bw = new BufferedWriter(fw);
            pw = new PrintWriter(bw);

            pw.println(str);
            pw.flush();
        }catch (Exception e) {
            e.printStackTrace();
            System.out.println("\nerror: 写入已完成的文件到 "+filename+" 失败!");
        } finally {
            try {
                pw.close();
                bw.close();
                fw.close();
            } catch (IOException io) {// can't do anything }
            }
        }
    }

    private void copyWorker(int id, String dstDir) {
        short loopTime=0;
        while(true) { // 无限循环，等待任务
            try {
                String taskFile;
                if ((taskFile = taskFileList.poll()) != null) {
                    File srcFile = new File(taskFile);
                    File dstFile = new File(dstDir,srcFile.getName());
                    File mountp=mountPoint(dstDir);
//                    System.out.println(mountp+" 空闲空间(GB)："+DirCopy.byteToGB(mountp.getUsableSpace()));
                    if(mountp.getUsableSpace()<srcFile.length()) {
                        System.out.println("空闲空间: "+mountp.getUsableSpace()
                                +"目的文件："+dstFile+" 源文件大小："+srcFile.length());
                        System.out.println("磁盘"+dstDir+"空间不足，线程"+id+"退出！");
                        break;
                    }
                    System.out.println("id="+id + " " + dstFile);
                    copyByBufferedInOutStream(srcFile,dstFile);
                    if(resume) writeStringToFile(finishedLogPath,taskFile);
                } else {
                    ++loopTime;
                    System.out.println(dstDir+" id="+id+"无任务，休眠");
                    TimeUnit.SECONDS.sleep(refreshInterval+1);
                    System.out.println("lootTime:"+loopTime);
                    if(!loop && loopTime>1 && taskFileList.isEmpty()) return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static File mountPoint(String p) throws IOException {
        FileStore fs = Files.getFileStore(Path.of(p));
        Path temp = Path.of(p).toAbsolutePath();
        Path mountpoint = temp;

        while( (temp = temp.getParent()) != null && fs.equals(Files.getFileStore(temp)) ) {
            mountpoint = temp;
        }
        return new File(mountpoint.toString());
    }

    public static void copyByBufferedInOutStream(File scrFile,File destFile) throws IOException {
        byte[] bytes=new byte[8*1024];
        try (InputStream in = new BufferedInputStream(new FileInputStream(scrFile))) {
            OutputStream out = new BufferedOutputStream(new FileOutputStream(destFile));
            int count;
            while ((count = in.read(bytes)) > 0) {
                out.write(bytes,0,count);
            }
        }
    }

    private void printDisksInfo(File[] roots) {
        System.out.println("所有磁盘信息如下：");
        for (File root : roots) {
            root.toString();
            System.out.print(root);
            long usable=root.getUsableSpace();
            long total=root.getTotalSpace();
            long used = total-usable;
            System.out.printf("  总空间: %.2fGB ",byteToGB(usable));
            System.out.printf("  可用空间: %.2fGB ",byteToGB(usable));
            System.out.printf("  已用空间: %.2fGB\n",byteToGB(used));
        }
    }

    @Override
    public String toString() {
        return "配置如下{\n" +
                " 刷新间隔=" + refreshInterval +
                "\n 源目录='" + srcDirPath +
                "\n 写入目录='" + dstDirStack +
                "\n 线程数=" + maxThreads+
                '}';
    }
}
