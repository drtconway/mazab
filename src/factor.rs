use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Factor {
    pub levels: Vec<String>,
    index: HashMap<String, usize>,
}

impl Factor {
    pub fn new() -> Factor {
        Factor {
            levels: Vec::new(),
            index: HashMap::new(),
        }
    }

    pub fn from_levels(levels: &[&str]) -> Factor {
        let levels = Vec::from_iter(levels.iter().map(|s| s.to_string()));
        let mut index = HashMap::new();
        for i in 0..levels.len() {
            index.insert(levels[i].to_string(), i);
        }
        Factor {levels, index}
    }

    pub fn from_pairs(pairs: &[(usize, String)]) -> Factor {
        let mut levels = Vec::new();
        let mut index = HashMap::new();
        for pair in pairs {
            while levels.len() <= pair.0 {
                levels.push("".to_string());
            }
            levels[pair.0] = pair.1.to_string();
            index.insert(pair.1.to_string(), pair.0);
        }
        Factor { levels, index }
    }

    pub fn get_or_add(&mut self, x: &str) -> usize {
        let n = self.index.len();
        match self.index.get(x) {
            None => {
                self.levels.push(x.to_string());
                self.index.insert(x.to_string(), n);
                n
            },
            Some(y) => *y,
        }
    }

    pub fn get(&self, x: &str) -> Option<usize> {
        self.index.get(x).cloned()
    }
}
