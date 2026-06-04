// <!-- Copy Code -->
function copyCode(elementId, button) {
    // Récupérer le texte du bloc de code cible
    const codeText = document.getElementById(elementId).innerText;
    
    // Utiliser l'API Clipboard pour copier le texte
    navigator.clipboard.writeText(codeText).then(() => {
        // Changer temporairement le bouton pour feedback visuel
        const icon = button.querySelector('i');
        const text = button.querySelector('span');
        
        icon.className = 'fas fa-check text-emerald-400';
        text.innerText = 'Copied!';
        button.classList.add('border-emerald-500/50');
        
        // Remettre l'état initial après 2 secondes
        setTimeout(() => {
            icon.className = 'far fa-copy';
            text.innerText = 'Copy';
            button.classList.remove('border-emerald-500/50');
        }, 2000);
    }).catch(err => {
        console.error('Erreur lors de la copie : ', err);
    });
}
