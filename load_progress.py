import progressbar
# from progressbar import Timer, Bar, ETA, ProgressBar


class Progress:
    position = 0
    def __init__(self, max, custom_text):    
        self.custom_text = custom_text    
        self.max = max  
        self.position = self.position
        self.widgets =  [f"{custom_text}: [",
                # progressbar.Timer(format= 'elapsed time: %(elapsed)s'),                         
                progressbar.Timer(format= '%(elapsed)s'),
                '] [',
                progressbar.Counter(format='%(value)d'),' / ', str(self.max),']',
                progressbar.Bar('#')
                # progressbar.ETA(), ') ',
                # progressbar.GranularBar()
                ]
        
        self.bar = progressbar.ProgressBar(max_value=max, 
                                    widgets=self.widgets).start()                
    def update(self):
        self.position += 1
        self.bar.update(self.position)