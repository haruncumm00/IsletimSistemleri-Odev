import csv
import threading
import os
from copy import deepcopy
from collections import deque

# ==========================================
# AYARLAR VE SABİTLER
# ==========================================
CONTEXT_SWITCH_TIME = 0.001  
TIME_QUANTUM = 4             

PRIORITY_MAP = {
    "high": 1,
    "normal": 2,
    "low": 3
}

# ==========================================
# PROCESS SINIFI
# ==========================================
class Process:
    def __init__(self, pid, arrival, burst, priority_str):
        self.pid = pid
        self.arrival = int(arrival)
        self.burst = int(burst)
        self.initial_burst = int(burst) # Bekleme süresi hesabı için gerekli
        self.remaining = int(burst)
        # Priority'yi sayıya çeviriyoruz (Düşük sayı = Yüksek öncelik)
        self.priority = PRIORITY_MAP.get(str(priority_str).lower(), 4)
        
        self.start_time = -1
        self.finish_time = 0
        self.waiting_time = 0
        self.turnaround_time = 0

    def __repr__(self):
        return f"{self.pid}"

# ==========================================
# DOSYA OKUMA VE SONUÇ YAZMA
# ==========================================
def read_processes(filename):
    processes = []
    if not os.path.exists(filename):
        print(f"HATA: '{filename}' dosyası bulunamadı.")
        return []

    try:
        with open(filename, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None) # Başlığı atla
            if header:
                for row in reader:
                    if len(row) >= 4:
                        p = Process(row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip())
                        processes.append(p)
    except Exception as e:
        print(f"HATA: Dosya okunurken sorun oluştu: {e}")
        
    return processes

def save_results(case_name, algo_name, processes, timeline, context_switches):
    """
    Ödevin istediği tüm metrikleri hesaplar ve dosyaya yazar.
    """
    # 1. Metrik Hesaplamaları
    total_waiting = 0
    total_turnaround = 0
    max_completion_time = 0
    total_burst_time = 0

    for p in processes:
        p.turnaround_time = p.finish_time - p.arrival
        p.waiting_time = p.turnaround_time - p.initial_burst
        
        # Bekleme süresi negatif olamaz (güvenlik kontrolü)
        if p.waiting_time < 0: p.waiting_time = 0
        
        total_waiting += p.waiting_time
        total_turnaround += p.turnaround_time
        total_burst_time += p.initial_burst
        
        if p.finish_time > max_completion_time:
            max_completion_time = p.finish_time

    n = len(processes)
    avg_waiting = total_waiting / n if n > 0 else 0
    avg_turnaround = total_turnaround / n if n > 0 else 0
    
    max_waiting = max((p.waiting_time for p in processes), default=0)
    max_turnaround = max((p.turnaround_time for p in processes), default=0)

    # Verimlilik Hesabı (Toplam Burst / Toplam Geçen Süre + Switch Maliyeti)
    total_duration = max_completion_time + (context_switches * CONTEXT_SWITCH_TIME)
    efficiency = (total_burst_time / total_duration) * 100 if total_duration > 0 else 0

    # Throughput Hesabı
    throughput = {}
    for T in [50, 100, 150, 200]:
        count = sum(1 for p in processes if p.finish_time <= T)
        throughput[T] = count

    # Dosya Adı Oluşturma
    clean_name = algo_name.replace(" ", "_")
    output_filename = f"{case_name}_Sonuc_{clean_name}.txt"
    
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write(f"DURUM: {case_name} | ALGORITMA: {algo_name}\n")
        f.write("="*50 + "\n\n")

        # a) Zaman Tablosu (İstenen [ 0 ] - - P001 - - [ 4 ] formatı)
        f.write("a) Zaman Tablosu [Gantt Chart]\n")
        for start, end, pid in timeline:
            f.write(f"[ {start:3} ] - - {pid:^4} - - [ {end:3} ]\n")
        
        f.write("\n" + "-"*40 + "\n")
        f.write("b) Bekleme Sureleri (Waiting Time)\n")
        f.write(f"   Ortalama: {avg_waiting:.3f}\n")
        f.write(f"   Maksimum: {max_waiting:.3f}\n")

        f.write("\nc) Tamamlanma Sureleri (Turnaround Time)\n")
        f.write(f"   Ortalama: {avg_turnaround:.3f}\n")
        f.write(f"   Maksimum: {max_turnaround:.3f}\n")

        f.write("\nd) Is Tamamlama Sayisi (Throughput)\n")
        for T, count in throughput.items():
            f.write(f"   T={T}: {count}\n")

        f.write("\ne) Ortalama CPU Verimliligi\n")
        f.write(f"   Deger: %{efficiency:.3f}\n")

        f.write("\nf) Toplam Baglam Degistirme Sayisi\n")
        f.write(f"   Sayi: {context_switches}\n")

    print(f"--> {output_filename} oluşturuldu.")

# ==========================================
# ALGORİTMALAR
# ==========================================

def fcfs(processes, case_name):
    processes.sort(key=lambda x: x.arrival)
    time = 0
    timeline = []
    switches = 0
    last_pid = None

    for p in processes:
        if time < p.arrival:
            timeline.append((time, p.arrival, "IDLE"))
            time = p.arrival
        
        if last_pid is not None and last_pid != p.pid:
            switches += 1
        
        start = time
        time += p.burst
        p.start_time = start
        p.finish_time = time
        timeline.append((start, time, p.pid))
        last_pid = p.pid

    save_results(case_name, "FCFS", processes, timeline, switches)

def sjf_non_preemptive(processes, case_name):
    processes.sort(key=lambda x: x.arrival)
    time = 0
    completed = 0
    n = len(processes)
    timeline = []
    switches = 0
    last_pid = None
    
    active_processes = deepcopy(processes)
    
    while completed < n:
        available = [p for p in active_processes if p.arrival <= time and p.remaining > 0]
        
        if not available:
            timeline.append((time, time+1, "IDLE"))
            time += 1
            continue
            
        current = min(available, key=lambda x: x.burst)
        
        if last_pid is not None and last_pid != current.pid:
            switches += 1
            
        start = time
        time += current.burst
        current.remaining = 0
        current.finish_time = time
        
        timeline.append((start, time, current.pid))
        last_pid = current.pid
        completed += 1

    process_map = {p.pid: p for p in active_processes}
    for p in processes: p.finish_time = process_map[p.pid].finish_time

    save_results(case_name, "SJF NonPreemptive", processes, timeline, switches)

def sjf_preemptive(processes, case_name):
    time = 0
    completed = 0
    n = len(processes)
    timeline = []
    switches = 0
    last_pid = None
    
    proc_list = deepcopy(processes)
    
    while completed < n:
        ready = [p for p in proc_list if p.arrival <= time and p.remaining > 0]
        
        if not ready:
            if timeline and timeline[-1][2] == "IDLE":
                s, e, pid = timeline.pop()
                timeline.append((s, time + 1, pid))
            else:
                timeline.append((time, time + 1, "IDLE"))
            time += 1
            continue
        
        current = min(ready, key=lambda x: x.remaining)
        
        if last_pid is not None and last_pid != current.pid:
            switches += 1
        
        # Timeline optimizasyonu (Parçalanmayı önlemek için birleştirme)
        if timeline and timeline[-1][2] == current.pid:
            start, end, pid = timeline.pop()
            timeline.append((start, time + 1, pid))
        else:
            timeline.append((time, time + 1, current.pid))
            
        current.remaining -= 1
        last_pid = current.pid
        time += 1
        
        if current.remaining == 0:
            current.finish_time = time
            completed += 1

    res_map = {p.pid: p for p in proc_list}
    for p in processes: p.finish_time = res_map[p.pid].finish_time

    save_results(case_name, "SJF Preemptive", processes, timeline, switches)

def priority_non_preemptive(processes, case_name):
    time = 0
    completed = 0
    n = len(processes)
    timeline = []
    switches = 0
    last_pid = None
    
    active_processes = deepcopy(processes)
    
    while completed < n:
        ready = [p for p in active_processes if p.arrival <= time and p.remaining > 0]
        
        if not ready:
            timeline.append((time, time+1, "IDLE"))
            time += 1
            continue
            
        current = min(ready, key=lambda x: x.priority)
        
        if last_pid is not None and last_pid != current.pid:
            switches += 1
            
        start = time
        time += current.remaining
        current.remaining = 0
        current.finish_time = time
        
        timeline.append((start, time, current.pid))
        last_pid = current.pid
        completed += 1

    process_map = {p.pid: p for p in active_processes}
    for p in processes: p.finish_time = process_map[p.pid].finish_time

    save_results(case_name, "Priority NonPreemptive", processes, timeline, switches)

def priority_preemptive(processes, case_name):
    time = 0
    completed = 0
    n = len(processes)
    timeline = []
    switches = 0
    last_pid = None
    
    proc_list = deepcopy(processes)
    
    while completed < n:
        ready = [p for p in proc_list if p.arrival <= time and p.remaining > 0]
        
        if not ready:
            if timeline and timeline[-1][2] == "IDLE":
                s, e, pid = timeline.pop()
                timeline.append((s, time + 1, pid))
            else:
                timeline.append((time, time + 1, "IDLE"))
            time += 1
            continue
        
        current = min(ready, key=lambda x: x.priority)
        
        if last_pid is not None and last_pid != current.pid:
            switches += 1
        
        if timeline and timeline[-1][2] == current.pid:
            start, end, pid = timeline.pop()
            timeline.append((start, time + 1, pid))
        else:
            timeline.append((time, time + 1, current.pid))
            
        current.remaining -= 1
        last_pid = current.pid
        time += 1
        
        if current.remaining == 0:
            current.finish_time = time
            completed += 1

    res_map = {p.pid: p for p in proc_list}
    for p in processes: p.finish_time = res_map[p.pid].finish_time

    save_results(case_name, "Priority Preemptive", processes, timeline, switches)

def round_robin(processes, case_name):
    time = 0
    queue = []
    timeline = []
    switches = 0
    last_pid = None
    
    proc_list = deepcopy(processes)
    proc_list.sort(key=lambda x: x.arrival)
    
    i = 0 
    n = len(proc_list)
    completed = 0
    
    # İlk gelenleri kuyruğa al
    while i < n and proc_list[i].arrival <= time:
        queue.append(proc_list[i])
        i += 1
        
    while completed < n:
        if not queue:
            # Kuyruk boş ama işlem var (idle)
            if i < n:
                if timeline and timeline[-1][2] == "IDLE":
                     s, e, pid = timeline.pop()
                     timeline.append((s, time + 1, pid))
                else:
                    timeline.append((time, time+1, "IDLE"))
                time += 1
                while i < n and proc_list[i].arrival <= time:
                    queue.append(proc_list[i])
                    i += 1
            continue

        current = queue.pop(0)
        
        if last_pid is not None and last_pid != current.pid:
            switches += 1
            
        exec_time = min(TIME_QUANTUM, current.remaining)
        
        start = time
        time += exec_time
        timeline.append((start, time, current.pid))
        
        current.remaining -= exec_time
        last_pid = current.pid
        
        # Bu sürede yeni gelenler var mı?
        while i < n and proc_list[i].arrival <= time:
            queue.append(proc_list[i])
            i += 1
            
        if current.remaining > 0:
            queue.append(current) 
        else:
            current.finish_time = time
            completed += 1

    res_map = {p.pid: p for p in proc_list}
    for p in processes: p.finish_time = res_map[p.pid].finish_time

    save_results(case_name, "Round Robin", processes, timeline, switches)

# ==========================================
# THREAD YÖNETİCİSİ VE MAIN
# ==========================================
def run_simulation(filename, case_label):
    print(f"\n>>> İŞLENİYOR: {filename} ({case_label})")
    base_processes = read_processes(filename)
    
    if not base_processes:
        print(f"UYARI: {filename} okunamadı veya boş!")
        return

    # Thread Listesi
    # Her algoritmaya 'deepcopy' ile temiz veri ve 'case_label' gönderiyoruz
    threads = []
    threads.append(threading.Thread(target=fcfs, args=(deepcopy(base_processes), case_label)))
    threads.append(threading.Thread(target=sjf_preemptive, args=(deepcopy(base_processes), case_label)))
    threads.append(threading.Thread(target=sjf_non_preemptive, args=(deepcopy(base_processes), case_label)))
    threads.append(threading.Thread(target=round_robin, args=(deepcopy(base_processes), case_label)))
    threads.append(threading.Thread(target=priority_preemptive, args=(deepcopy(base_processes), case_label)))
    threads.append(threading.Thread(target=priority_non_preemptive, args=(deepcopy(base_processes), case_label)))
    
    # Eş zamanlı başlat
    for t in threads: t.start()
    # Bitmelerini bekle
    for t in threads: t.join()
    
    print(f">>> {case_label} tamamlandı. Dosyalar oluşturuldu.\n")

if __name__ == "__main__":
    # Case 1 ve Case 2 için ayrı ayrı çalıştır
    run_simulation("case1.csv", "Case1")
    run_simulation("case2.csv", "Case2")