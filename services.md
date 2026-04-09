<!DOCTYPE html>
<html lang="en" class="scroll-smooth">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Our Services | DB Analytics Tools</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Fira+Code:wght@400;500&display=swap');
        
        :root {
            --bg-main: #0d1117; 
            --bg-card: #161b22;
            --border-color: #30363d;
            --accent-blue: #2f81f7;
        }

        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-main);
            color: #c9d1d9;
        }

        .glass-header {
            background: rgba(13, 17, 23, 0.8);
            backdrop-filter: blur(8px);
            border-bottom: 1px solid var(--border-color);
        }

        .service-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }

        .service-card:hover {
            border-color: var(--accent-blue);
            transform: translateY(-5px);
            box-shadow: 0 10px 30px -15px rgba(47, 129, 247, 0.3);
        }

        .icon-box {
            background: linear-gradient(135deg, rgba(47, 129, 247, 0.1) 0%, rgba(47, 129, 247, 0.05) 100%);
            border: 1px solid rgba(47, 129, 247, 0.2);
        }

        .footer-link:hover {
            color: white;
            text-decoration: none;
        }
    </style>
</head>
<body>

    <!-- NAVIGATION -->
    <nav class="fixed w-full z-50 glass-header">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="flex items-center justify-between h-16">
                <div class="flex items-center gap-3">
                    <a href="index.html">
                        <div class="w-8 h-8 bg-blue-600 rounded flex items-center justify-center text-white font-bold text-xs">DB</div>
                    </a>
                    <span class="text-lg font-semibold text-white hidden sm:block tracking-tight">DB Analytics Tools</span>
                </div>
                <div class="flex items-center space-x-6 text-sm font-medium">
                    <a href="index.html" class="hover:text-white transition">Home</a>
                    <a href="services.html" class="text-blue-400">Services</a>
                    <a href="docs.html" class="hover:text-white transition">Docs</a>
                    <a href="https://github.com/joekakone/db-analytics-tools" target="_blank" class="text-xl hover:text-white transition">
                        <i class="fab fa-github"></i>
                    </a>
                </div>
            </div>
        </div>
    </nav>

    <!-- HEADER SECTION -->
    <section class="pt-32 pb-16 border-b border-slate-800 bg-slate-900/20">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
            <h1 class="text-4xl md:text-5xl font-extrabold text-white mb-4">Professional Data Services</h1>
            <p class="text-slate-400 max-w-2xl mx-auto text-lg">
                We combine our open-source framework with deep expertise to help you build robust, scalable, and insightful data ecosystems.
            </p>
        </div>
    </section>

    <!-- SERVICES GRID -->
    <section class="py-20">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
                
                <!-- Data Audit -->
                <div class="service-card p-8 rounded-2xl flex flex-col">
                    <div class="w-14 h-14 icon-box rounded-xl flex items-center justify-center text-blue-500 mb-6 text-2xl">
                        <i class="fas fa-clipboard-check"></i>
                    </div>
                    <h3 class="text-xl font-bold text-white mb-3">Data Audit</h3>
                    <p class="text-slate-400 text-sm leading-relaxed mb-6">
                        Evaluation of your current data health. We analyze data quality, consistency, and architecture to identify bottlenecks and security risks in your existing pipelines.
                    </p>
                    <ul class="text-xs space-y-2 mt-auto text-slate-500">
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> Quality Assessment</li>
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> Performance Benchmarking</li>
                    </ul>
                </div>

                <!-- Data Integration -->
                <div class="service-card p-8 rounded-2xl flex flex-col border-blue-500/30 shadow-lg">
                    <div class="w-14 h-14 icon-box rounded-xl flex items-center justify-center text-blue-500 mb-6 text-2xl">
                        <i class="fas fa-layer-group"></i>
                    </div>
                    <h3 class="text-xl font-bold text-white mb-3">Data Integration</h3>
                    <p class="text-slate-400 text-sm leading-relaxed mb-6">
                        End-to-end Data Warehouse Design and Implementation. We unify disparate sources (SQL, NoSQL, APIs) into a single source of truth using Star or Snowflake schemas.
                    </p>
                    <ul class="text-xs space-y-2 mt-auto text-slate-500">
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> DWH Architecture</li>
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> ETL/ELT Strategy</li>
                    </ul>
                </div>

                <!-- Apache Airflow -->
                <div class="service-card p-8 rounded-2xl flex flex-col">
                    <div class="w-14 h-14 icon-box rounded-xl flex items-center justify-center text-blue-500 mb-6 text-2xl">
                        <i class="fas fa-wind"></i>
                    </div>
                    <h3 class="text-xl font-bold text-white mb-3">Airflow Deployment</h3>
                    <p class="text-slate-400 text-sm leading-relaxed mb-6">
                        Setting up industrial-grade orchestration. We handle the deployment of Apache Airflow on your infrastructure and develop custom DAGs using the DB Analytics framework.
                    </p>
                    <ul class="text-xs space-y-2 mt-auto text-slate-500">
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> Cloud or On-Premise</li>
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> DAG Automation</li>
                    </ul>
                </div>

                <!-- Data Visualization -->
                <div class="service-card p-8 rounded-2xl flex flex-col">
                    <div class="w-14 h-14 icon-box rounded-xl flex items-center justify-center text-blue-500 mb-6 text-2xl">
                        <i class="fas fa-chart-pie"></i>
                    </div>
                    <h3 class="text-xl font-bold text-white mb-3">Data Visualization</h3>
                    <p class="text-slate-400 text-sm leading-relaxed mb-6">
                        Custom Excel & Power BI reporting. We transform raw data into interactive dashboards that empower decision-makers with real-time insights.
                    </p>
                    <ul class="text-xs space-y-2 mt-auto text-slate-500">
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> Interactive Dashboards</li>
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> Advanced DAX Measures</li>
                    </ul>
                </div>

                <!-- Analysis Services -->
                <div class="service-card p-8 rounded-2xl flex flex-col">
                    <div class="w-14 h-14 icon-box rounded-xl flex items-center justify-center text-blue-500 mb-6 text-2xl">
                        <i class="fas fa-cube"></i>
                    </div>
                    <h3 class="text-xl font-bold text-white mb-3">OLAP Cube Development</h3>
                    <p class="text-slate-400 text-sm leading-relaxed mb-6">
                        Development of Analysis Services (SSAS) cubes. Optimize multi-dimensional analysis for high-speed querying of massive business datasets.
                    </p>
                    <ul class="text-xs space-y-2 mt-auto text-slate-500">
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> Tabular & Multidimensional</li>
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> Query Optimization</li>
                    </ul>
                </div>

                <!-- Support & Training -->
                <div class="service-card p-8 rounded-2xl flex flex-col bg-blue-600/5 border-dashed">
                    <div class="w-14 h-14 icon-box rounded-xl flex items-center justify-center text-blue-500 mb-6 text-2xl">
                        <i class="fas fa-graduation-cap"></i>
                    </div>
                    <h3 class="text-xl font-bold text-white mb-3">Training & Support</h3>
                    <p class="text-slate-400 text-sm leading-relaxed mb-6">
                        Upskill your team on the DB Analytics framework or modern data engineering practices. Ongoing technical support for your critical pipelines.
                    </p>
                    <ul class="text-xs space-y-2 mt-auto text-slate-500">
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> On-site Workshops</li>
                        <li><i class="fas fa-check text-blue-500 mr-2"></i> SLA-based Support</li>
                    </ul>
                </div>

            </div>
        </div>
    </section>

    <!-- CALL TO ACTION -->
    <section class="py-20 bg-[#010409]">
        <div class="max-w-4xl mx-auto px-4 text-center border border-slate-800 rounded-3xl p-12 bg-gradient-to-b from-slate-900/50 to-transparent">
            <h2 class="text-3xl font-bold text-white mb-6">Ready to scale your data?</h2>
            <p class="text-slate-400 mb-10">Tell us about your project and we'll help you find the right architecture.</p>
            <div class="flex flex-wrap justify-center gap-4">
                <a href="mailto:contact@dbanalytics.tg" class="bg-blue-600 hover:bg-blue-700 text-white px-8 py-3 rounded-md font-bold transition">
                    Book a Free Consultation
                </a>
                <a href="tel:+22891518923" class="border border-slate-700 hover:bg-slate-800 text-white px-8 py-3 rounded-md font-bold transition">
                    Call +228 91 51 89 23
                </a>
            </div>
        </div>
    </section>

    <!-- FOOTER (Sync with Index) -->
    <footer class="pt-16 pb-12 bg-[#0d1117] border-t border-slate-900">
        <div class="max-w-7xl mx-auto px-4 text-center">
            <div class="flex items-center justify-center gap-3 mb-8">
                <div class="w-6 h-6 bg-blue-600 rounded flex items-center justify-center text-white font-bold text-[10px]">DB</div>
                <span class="text-slate-300 font-semibold tracking-tight">DB Analytics Tools</span>
            </div>
            <p class="text-slate-500 text-xs mb-8">© 2026 Lomé, Togo. Built with passion by Joseph Konka.</p>
            <div class="flex justify-center gap-6 text-slate-600 text-lg">
                <a href="https://github.com/joekakone" class="hover:text-blue-500 transition"><i class="fab fa-github"></i></a>
                <a href="#" class="hover:text-blue-500 transition"><i class="fab fa-linkedin"></i></a>
            </div>
        </div>
    </footer>

</body>
</html>
