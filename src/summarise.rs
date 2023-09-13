#[derive(Clone, Copy, Debug)]
pub struct Summariser {
    pub n: usize,
    sx: f64,
    sx2: f64,
}

impl Summariser {
    pub fn new() -> Summariser {
        Summariser {
            n: 0,
            sx: 0.0,
            sx2: 0.0,
        }
    }

    pub fn add(&mut self, x: f64) {
        self.n += 1;
        self.sx += x;
        self.sx2 += x * x;
    }

    pub fn add_multiple(&mut self, x: f64, n: usize) {
        self.n += n;
        self.sx += (n as f64) * x;
        self.sx2 += (n as f64) * x * x;
    }

    pub fn add_other(&mut self, other: &Summariser) {
        self.n += other.n;
        self.sx += other.sx;
        self.sx2 += other.sx2;
    }

    pub fn mean(&self) -> f64 {
        self.sx / (self.n as f64)
    }

    pub fn var(&self) -> f64 {
        let m = self.mean();
        self.sx2 / (self.n as f64) - m * m
    }

    pub fn sd(&self) -> f64 {
        if self.n == 0 {
            -1.0
        } else {
            self.var().sqrt()
        }
    }
}

impl Default for Summariser {
    fn default() -> Self {
        Self::new()
    }
}
