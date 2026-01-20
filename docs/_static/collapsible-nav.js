/**
 * Make sidebar toctree captions collapsible
 * Allows users to collapse/expand entire documentation sections
 * All sections collapsed by default, expand only the active section
 */

document.addEventListener('DOMContentLoaded', function() {
    // Find all caption elements in the sidebar
    const captions = document.querySelectorAll('.sidebar-tree .caption');

    captions.forEach(function(caption) {
        const captionText = caption.textContent.trim();

        // Check if this section contains the current page
        const nextElement = caption.nextElementSibling;
        let hasCurrentPage = false;

        if (nextElement && nextElement.tagName === 'UL') {
            // Check if any link in this section is the current page
            const currentLinks = nextElement.querySelectorAll('a.current');
            hasCurrentPage = currentLinks.length > 0;
        }

        // Collapse all sections by default, except the one containing current page
        if (!hasCurrentPage) {
            caption.classList.add('collapsed');
        }

        // Add click handler
        caption.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();

            // Toggle collapsed class
            this.classList.toggle('collapsed');
        });
    });
});
